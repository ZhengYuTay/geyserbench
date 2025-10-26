use crate::proto::geyser::CommitmentLevel;
use anyhow::{Context, Result, anyhow};
use serde::{
    Deserialize, Serialize,
    de::{self, Deserializer},
};
use std::{fs, path::Path};

const DEFAULT_ACCOUNT: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigToml {
    pub config: Config,
    pub endpoint: Vec<Endpoint>,
    #[serde(default)]
    pub backend: BackendSettings,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub transactions: i32,
    #[serde(
        alias = "account",
        default = "default_accounts",
        deserialize_with = "deserialize_accounts"
    )]
    pub accounts: Vec<String>,
    pub commitment: ArgsCommitment,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Endpoint {
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_token: Option<String>,
    pub kind: EndpointKind,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Default, Deserialize, Serialize, Clone)]
pub struct BackendSettings {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum EndpointKind {
    Yellowstone,
    Arpc,
    Thor,
    Shredstream,
    Shreder,
    Jetstream,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

impl ArgsCommitment {
    pub fn as_str(&self) -> &'static str {
        match self {
            ArgsCommitment::Processed => "processed",
            ArgsCommitment::Confirmed => "confirmed",
            ArgsCommitment::Finalized => "finalized",
        }
    }
}

impl EndpointKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            EndpointKind::Yellowstone => "yellowstone",
            EndpointKind::Arpc => "arpc",
            EndpointKind::Thor => "thor",
            EndpointKind::Shredstream => "shredstream",
            EndpointKind::Shreder => "shreder",
            EndpointKind::Jetstream => "jetstream",
        }
    }
}

impl ConfigToml {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Failed to read config {}", path))?;
        let config = toml::from_str(&content).map_err(|err| anyhow!(err))?;
        Ok(config)
    }

    pub fn create_default(path: &str) -> Result<Self> {
        let default_config = ConfigToml {
            config: Config {
                transactions: 1000,
                accounts: default_accounts(),
                commitment: ArgsCommitment::Processed,
            },
            endpoint: vec![
                Endpoint {
                    name: "grpc".to_string(),
                    url: "http://fra.corvus-labs.io:10101".to_string(),
                    x_token: None,
                    kind: EndpointKind::Yellowstone,
                },
                Endpoint {
                    name: "arpc".to_string(),
                    url: "http://fra.corvus-labs.io:20202".to_string(),
                    x_token: None,
                    kind: EndpointKind::Arpc,
                },
            ],
            backend: BackendSettings::default(),
        };

        let toml_string = toml::to_string_pretty(&default_config)
            .context("Failed to serialize default config")?;
        fs::write(path, toml_string)
            .with_context(|| format!("Failed to write default config {}", path))?;

        Ok(default_config)
    }

    pub fn load_or_create(path: &str) -> Result<Self> {
        if Path::new(path).exists() {
            Self::load(path)
        } else {
            Self::create_default(path)
        }
    }
}

fn default_accounts() -> Vec<String> {
    vec![DEFAULT_ACCOUNT.to_string()]
}

fn deserialize_accounts<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum AccountsField {
        Single(String),
        Multiple(Vec<String>),
    }

    let accounts = match AccountsField::deserialize(deserializer)? {
        AccountsField::Single(value) => vec![value],
        AccountsField::Multiple(values) => values,
    };

    if accounts.is_empty() {
        return Err(de::Error::custom("at least one account must be specified"));
    }

    Ok(accounts)
}
