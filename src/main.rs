mod config;
mod constants;
mod csv;
mod fswatch;
mod metrics;
mod model;
mod producer;
mod reject;
mod stream;
mod util;
mod worker;

#[macro_use]
extern crate lazy_static;
extern crate serde;

use std::{path::Path, time::Duration};

use stable_eyre::eyre::{Context, Result};
use tracing_subscriber::{filter, prelude::*};

use crate::csv::record_reader::RecordReader;
use crate::fswatch::create_fswatch;
use crate::metrics::STREAMS;
use crate::model::parse_stream_def;
use crate::reject::{create_reject_handle, create_replay_handles};
use crate::stream::configure_streams;
use crate::util::{create_async_channel, create_queue, TaskHandle};
use crate::worker::create_workers;
use crate::{config::cli_args, model::get_stream_schema};
use crate::{config::Config, metrics::create_metrics_endpoint};

#[tokio::main]
async fn main() -> Result<()> {
    stable_eyre::install()?;
    init_tracing()?;
    cli_command().await
}

async fn cli_command() -> Result<()> {
    let args = cli_args();
    match args.subcommand() {
        Some(("schema", _)) => {
            let schema = get_stream_schema()?;
            println!("{}", &schema);
        }
        Some(("validate", sub_args)) => {
            let path = Path::new(sub_args.value_of("PATH").unwrap());
            match parse_stream_def(path.to_owned()).await {
                Ok(model) => {
                    let reader = RecordReader::new(model.input.schema)?;
                    let avro_schema =
                        serde_json::to_string_pretty(&reader.schema(&model.output.topic))?;
                    println!("Config is valid, generated avro schema:\n{}", &avro_schema);
                }
                Err(err) => {
                    println!("Invalid config: {}", err);
                }
            }
        }
        Some(("run", _)) => {
            run().await?;
        }
        _ => unreachable!(
            "Exhausted list of subcommands and SubcommandRequiredElseHelp prevents `None`"
        ),
    }
    Ok(())
}

async fn run() -> Result<()> {
    let config = match Config::from_env() {
        Ok(ok) => ok,
        Err(err) => {
            println!("{}\n{}", err, Config::help_str());
            return Ok(());
        }
    };

    create_metrics_endpoint(config.metrics_port);
    create_tasks(&config).await
}

async fn create_tasks(config: &Config) -> Result<()> {
    let mut handle = TaskHandle::new();

    let (tx, rx) = create_queue(config.queue_capacity);
    let (tx_reject, rx_reject) = create_async_channel(config.queue_capacity);

    let (stream_reader_data, stream_writer_data, stream_reject_data) = configure_streams(
        &config
            .stream_dir
            .canonicalize()
            .wrap_err("unable to resolve stream dir")?,
        &config
            .root_dir
            .canonicalize()
            .wrap_err("unable to resolve root dir")?,
    )
    .await?;
    if stream_reader_data.is_empty() {
        log::warn!("no streams found in {:?}", &config.stream_dir);
        return Ok(());
    }
    for i in stream_writer_data.iter() {
        STREAMS.with_label_values(&[&i.name]).set(1);
    }

    create_reject_handle(
        rx_reject,
        &config.reject_dir,
        stream_reject_data,
        &mut handle,
    )
    .await?;
    create_replay_handles(
        config.kafka.config(),
        config.schema_registry.config(),
        &config.reject_dir,
        &tx_reject,
        stream_writer_data.clone(),
        &mut handle,
    )
    .await?;

    create_workers(
        config.kafka.config(),
        config.schema_registry.config(),
        rx,
        &tx_reject,
        stream_writer_data,
        config.workers,
        &mut handle,
    )?;
    let _watch = create_fswatch(
        tx,
        stream_reader_data,
        Duration::from_secs(2),
        config.queue_capacity,
        &mut handle,
    )
    .await?;

    handle.join().await
}

fn init_tracing() -> Result<()> {
    tracing_subscriber::registry()
        .with(console_subscriber::spawn())
        .with(tracing_subscriber::fmt::layer().with_filter(filter::LevelFilter::INFO))
        .init();
    Ok(())
}
