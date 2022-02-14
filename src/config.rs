use std::{path::PathBuf, time::Duration};

use ansi_term::Colour::Yellow;
use clap::{app_from_crate, arg, App, AppSettings, ArgMatches};
use envconfig::Envconfig;
use log::LevelFilter;
use rdkafka::ClientConfig;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use stable_eyre::eyre::Result;
use url::Url;

lazy_static! {
    static ref ENVIRONMENT_HELP_STRING: String = format!(
        "{}:
  F_ROOT_DIR (required): data root directory
  F_STREAM_DIR (required): stream config directory
  F_REJECT_DUR (required): reject record directory
  F_KAFKA_URL (required): kafka broker url
  F_SCHEMA_REGISTRY_URL (required): schema registry url
  F_METRICS_PORT (default = {:?}): metrics port
  F_LOG_LEVEL (default = {:?}): log level
  F_WORKERS (default = {:?}): worker number
  F_QUEUE_CAPACITY (default = {:?}): queue capacity
  F_FS_EVENT_DELAY_MS (default = {:?}): filesystem event delay in milliseconds",
        Yellow.paint("ENVIRONMENT VARIABLES"),
        get_default_metrics_port(),
        get_default_log_level(),
        get_default_workers(),
        get_default_queue_capacity(),
        get_default_fs_event_delay()
    );
}

#[derive(Envconfig)]
pub struct EnvironmentConfig {
    #[envconfig(from = "F_ROOT_DIR")]
    pub root_dir: PathBuf,
    #[envconfig(from = "F_STREAM_DIR")]
    pub stream_dir: PathBuf,
    #[envconfig(from = "F_REJECT_DIR")]
    pub reject_dir: PathBuf,
    #[envconfig(from = "F_KAFKA_URL")]
    pub kafka_url: Url,
    #[envconfig(from = "F_SCHEMA_REGISTRY_URL")]
    pub schema_registry_url: Url,
    #[envconfig(from = "F_METRICS_PORT", default = "9101")]
    pub metrics_port: u16,
    #[envconfig(from = "F_LOG_LEVEL", default = "Info")]
    pub log_level: LevelFilter,
    #[envconfig(from = "F_WORKERS", default = "2")]
    pub workers: usize,
    #[envconfig(from = "F_QUEUE_CAPACITY", default = "32767")]
    pub queue_capacity: usize,
    #[envconfig(from = "F_FS_EVENT_DELAY_MS", default = "200")]
    pub fs_event_delay: u64,
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
    pub reject_dir: PathBuf,
    pub metrics_port: u16,
    pub log_level: LevelFilter,
    pub workers: usize,
    pub queue_capacity: usize,
    pub fs_event_delay: Duration,
}

impl Config {
    pub fn new(env: EnvironmentConfig) -> Self {
        Self {
            kafka: KafkaProducerConfig::new(&env.kafka_url),
            schema_registry: SchemaRegistryConfig::new(&env.schema_registry_url),
            root_dir: env.root_dir,
            stream_dir: env.stream_dir,
            reject_dir: env.reject_dir,
            metrics_port: env.metrics_port,
            log_level: env.log_level,
            workers: env.workers,
            queue_capacity: env.queue_capacity,
            fs_event_delay: Duration::from_millis(env.fs_event_delay),
        }
    }

    pub fn from_env() -> Result<Self> {
        Ok(Self::new(EnvironmentConfig::init_from_env()?))
    }

    pub fn help_str() -> &'static str {
        &*ENVIRONMENT_HELP_STRING
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

fn get_default_metrics_port() -> String {
    "9109".to_string()
}

fn get_default_log_level() -> String {
    "Info".to_string()
}

fn get_default_workers() -> String {
    num_cpus::get().to_string()
}

fn get_default_queue_capacity() -> String {
    "1000".to_string()
}

fn get_default_fs_event_delay() -> String {
    "200".to_string()
}
