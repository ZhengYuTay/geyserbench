use std::{collections::HashMap, error::Error, sync::atomic::Ordering};

use futures_util::{sink::SinkExt, stream::StreamExt};
use tokio::task;
use tonic::transport::ClientTlsConfig;
use tracing::{Level, error, info, warn};

use crate::proto::geyser::{
    CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, subscribe_update::UpdateOneof,
};

use crate::{
    config::{Config, Endpoint},
    utils::{TransactionData, get_current_timestamp, open_log_file, write_log_entry},
};

use super::{
    GeyserProvider, ProviderContext,
    common::{
        TransactionAccumulator, build_signature_envelope, enqueue_signature, fatal_connection_error,
    },
    yellowstone_client::GeyserGrpcClient,
};

pub struct YellowstoneProvider;

impl GeyserProvider for YellowstoneProvider {
    fn process(
        &self,
        endpoint: Endpoint,
        config: Config,
        context: ProviderContext,
    ) -> task::JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
        task::spawn(async move { process_yellowstone_endpoint(endpoint, config, context).await })
    }
}

async fn process_yellowstone_endpoint(
    endpoint: Endpoint,
    config: Config,
    context: ProviderContext,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let ProviderContext {
        shutdown_tx,
        mut shutdown_rx,
        start_wallclock_secs,
        start_instant,
        comparator,
        signature_tx,
        shared_counter,
        shared_shutdown,
        target_transactions,
        total_producers,
        progress,
    } = context;

    let signature_sender = signature_tx;
    let endpoint_name = endpoint.name.clone();
    let mut log_file = if tracing::enabled!(Level::TRACE) {
        Some(open_log_file(&endpoint_name)?)
    } else {
        None
    };

    let endpoint_url = endpoint.url.clone();
    let endpoint_token = endpoint
        .x_token
        .clone()
        .filter(|token| !token.trim().is_empty());

    info!(endpoint = %endpoint_name, url = %endpoint_url, "Connecting");

    let builder = GeyserGrpcClient::build_from_shared(endpoint_url.clone())
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    let builder = if let Some(token) = endpoint_token {
        builder
            .x_token(Some(token))
            .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err))
    } else {
        builder
    };
    let builder = builder
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));
    let mut client = builder
        .connect()
        .await
        .unwrap_or_else(|err| fatal_connection_error(&endpoint_name, err));

    info!(endpoint = %endpoint_name, "Connected");

    let commitment: CommitmentLevel = config.commitment.into();

    let accounts_filters = HashMap::from([(
        "account".to_string(),
        SubscribeRequestFilterAccounts {
            account: config.accounts.clone(),
            owner: vec![],
            filters: vec![],
            nonempty_txn_signature: Some(true),
        },
    )]);

    let transactions_filters = HashMap::from([(
        "account".to_string(),
        SubscribeRequestFilterTransactions {
            account_include: config.accounts.clone(),
            account_exclude: vec![],
            account_required: vec![],
            ..Default::default()
        },
    )]);

    let (mut subscribe_tx, mut stream) = client
        .subscribe_with_request(Some(SubscribeRequest {
            slots: HashMap::default(),
            accounts: accounts_filters,
            transactions: transactions_filters,
            transactions_status: HashMap::default(),
            entry: HashMap::default(),
            blocks: HashMap::default(),
            blocks_meta: HashMap::default(),
            commitment: Some(commitment as i32),
            accounts_data_slice: Vec::default(),
            ping: None,
            from_slot: None,
        }))
        .await?;

    let mut accumulator = TransactionAccumulator::new();
    let mut transaction_count = 0usize;

    let mut record_signature = |signature: String| -> Result<(), Box<dyn Error + Send + Sync>> {
        let wallclock = get_current_timestamp();
        let elapsed = start_instant.elapsed();

        if let Some(file) = log_file.as_mut() {
            write_log_entry(file, wallclock, &endpoint_name, &signature)?;
        }

        let tx_data = TransactionData {
            wallclock_secs: wallclock,
            elapsed_since_start: elapsed,
            start_wallclock_secs,
        };

        let updated = accumulator.record(signature.clone(), tx_data.clone());

        if updated
            && let Some(envelope) = build_signature_envelope(
                &comparator,
                &endpoint_name,
                &signature,
                tx_data,
                total_producers,
            )
        {
            if let Some(target) = target_transactions {
                let shared = shared_counter.fetch_add(1, Ordering::AcqRel) + 1;
                if let Some(tracker) = progress.as_ref() {
                    tracker.record(shared);
                }
                if shared >= target && !shared_shutdown.swap(true, Ordering::AcqRel) {
                    info!(endpoint = %endpoint_name, target, "Reached shared signature target; broadcasting shutdown");
                    let _ = shutdown_tx.send(());
                }
            }

            if let Some(sender) = signature_sender.as_ref() {
                enqueue_signature(sender, &endpoint_name, &signature, envelope);
            }
        }

        Ok(())
    };

    loop {
        tokio::select! { biased;
            _ = shutdown_rx.recv() => {
                info!(endpoint = %endpoint_name, "Received stop signal");
                break;
            }

            message = stream.next() => {
                match message {
                    Some(Ok(msg)) => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Account(account_update)) => {
                                let Some(info) = account_update.account.as_ref() else { continue };
                                let Some(signature_bytes) = info.txn_signature.as_ref() else {
                                    warn!(endpoint = %endpoint_name, "Account update missing txn signature");
                                    continue;
                                };

                                record_signature(bs58::encode(signature_bytes).into_string())?;
                                transaction_count += 1;
                            },
                            Some(UpdateOneof::Transaction(tx_msg)) => {
                                let Some(tx) = tx_msg.transaction.as_ref() else { continue };
                                let Some(signature_bytes) = tx.transaction.as_ref()
                                    .and_then(|t| t.signatures.first()) else {
                                    warn!(endpoint = %endpoint_name, "Transaction update missing signature");
                                    continue;
                                };

                                record_signature(bs58::encode(signature_bytes).into_string())?;
                                transaction_count += 1;
                            },
                            Some(UpdateOneof::Ping(_)) => {
                                subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: 1 }),
                                        ..Default::default()
                                    })
                                    .await?;
                            },
                            _ => {}
                        }
                    },
                    Some(Err(e)) => {
                        error!(endpoint = %endpoint_name, error = ?e, "Error receiving message from stream");
                        break;
                    },
                    None => {
                        info!(endpoint = %endpoint_name, "Stream closed by server");
                        break;
                    }
                }
            }
        }
    }

    let unique_signatures = accumulator.len();
    let collected = accumulator.into_inner();
    comparator.add_batch(&endpoint_name, collected);
    info!(
        endpoint = %endpoint_name,
        total_transactions = transaction_count,
        unique_signatures,
        "Stream closed after dispatching transactions"
    );
    Ok(())
}
