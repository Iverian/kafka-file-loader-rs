use std::path::PathBuf;

use ansi_term::Colour::Yellow;
use clap::{app_from_crate, arg, App, AppSettings, ArgMatches};
use envconfig::Envconfig;
use eyre::Result;
use log::LevelFilter;
use rdkafka::ClientConfig;
use schema_registry_converter::blocking::schema_registry::SrSettings;
use url::Url;

lazy_static! {
    pub static ref ENVIRONMENT_HELP_STRING: String = format!(
        "{}:
  F_ROOT_DIR (required): data root directory
  F_STREAM_DIR (required): stream config directory
  F_REJECT_DB (required): reject sqlite database path
  F_KAFKA_URL (required): kafka broker url
  F_SCHEMA_REGISTRY_URL (required): schema registry url
  F_METRICS_PORT (default = {:?}): metrics port
  F_LOG_LEVEL (default = {:?}): log level
  F_WORKERS (default = {:?}): worker number
  F_QUEUE_CAPACITY (default = {:?}): queue capacity",
        Yellow.paint("ENVIRONMENT VARIABLES"),
        get_default_metrics_port(),
        get_default_log_level(),
        get_default_workers(),
        get_default_queue_capacity()
    );
}

#[derive(Envconfig)]
pub struct EnvironmentConfig {
    #[envconfig(from = "F_ROOT_DIR")]
    pub root_dir: PathBuf,
    #[envconfig(from = "F_STREAM_DIR")]
    pub stream_dir: PathBuf,
    #[envconfig(from = "F_REJECT_DB")]
    pub reject_db: PathBuf,
    #[envconfig(from = "F_KAFKA_URL")]
    pub kafka_url: Url,
    #[envconfig(from = "F_SCHEMA_REGISTRY_URL")]
    pub schema_registry_url: Url,
    #[envconfig(from = "F_METRICS_PORT", default = "get_default_metrics_port()")]
    pub metrics_port: u16,
    #[envconfig(from = "F_LOG_LEVEL", default = "get_default_log_level()")]
    pub log_level: LevelFilter,
    #[envconfig(from = "F_WORKERS", default = "get_default_workers()")]
    pub workers: usize,
    #[envconfig(from = "F_QUEUE_CAPACITY", default = "get_default_queue_capacity()")]
    pub queue_capacity: u64,
}

pub struct KafkaProducerConfig {
    config: ClientConfig,
}

pub struct SchemaRegistryConfig {
    config: SrSettings,
}

pub struct Config {
    pub kafka: KafkaProducerConfig,
    pub schema_registry: SchemaRegistryConfig,
    pub root_dir: PathBuf,
    pub stream_dir: PathBuf,
    pub reject_db: PathBuf,
    pub metrics_port: u16,
    pub log_level: LevelFilter,
    pub workers: usize,
    pub queue_capacity: u64,
}

impl Config {
    pub fn new() -> Result<Self> {
        let env = EnvironmentConfig::init_from_env()?;
        Ok(Self {
            kafka: KafkaProducerConfig::new(&env.kafka_url),
            schema_registry: SchemaRegistryConfig::new(&env.schema_registry_url),
            root_dir: env.root_dir.canonicalize()?,
            stream_dir: env.stream_dir.canonicalize()?,
            reject_db: env.reject_db.canonicalize()?,
            metrics_port: env.metrics_port,
            log_level: env.log_level,
            workers: env.workers,
            queue_capacity: env.queue_capacity,
        })
    }
}

impl KafkaProducerConfig {
    pub fn new(url: &Url) -> Self {
        let mut result = ClientConfig::new();
        for p in url.query_pairs() {
            result.set(p.0, p.1);
        }
        Self { config: result }
    }

    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}

impl SchemaRegistryConfig {
    pub fn new(url: &Url) -> Self {
        Self {
            config: SrSettings::new(url.to_string()),
        }
    }

    pub fn config(&self) -> &SrSettings {
        &self.config
    }
}

pub fn cli_args() -> ArgMatches {
    let help_str = format!("Run loader\n\n{}\n", *ENVIRONMENT_HELP_STRING);
    app_from_crate!()
        .global_setting(AppSettings::UseLongFormatForHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand(App::new("schema").about("Print config schema"))
        .subcommand(
            App::new("validate")
                .about("Validate config schema")
                .arg(arg!([PATH])),
        )
        .subcommand(App::new("run").about(&*help_str))
        .get_matches()
}

fn get_default_metrics_port() -> u16 {
    9109
}

fn get_default_log_level() -> LevelFilter {
    LevelFilter::Info
}

fn get_default_workers() -> usize {
    num_cpus::get()
}

fn get_default_queue_capacity() -> u64 {
    1000
}
