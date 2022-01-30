mod config;
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

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::Path,
    time::Duration,
};

use eyre::Result;
use model::parse_stream_def;

use crate::config::Config;
use crate::config::ENVIRONMENT_HELP_STRING;
use crate::csv::record_reader::RecordReader;
use crate::fswatch::create_fswatch;
use crate::metrics::STREAMS;
use crate::reject::{configure_reject_db, create_reject_handle, create_replay_handles};
use crate::stream::configure_streams;
use crate::util::{configure_logging, create_queue, ThreadWaiter};
use crate::worker::create_workers;
use crate::{config::cli_args, model::get_stream_schema};

fn main() -> Result<()> {
    let args = cli_args();
    match args.subcommand() {
        Some(("schema", _)) => {
            let schema = get_stream_schema()?;
            println!("{}", &schema);
        }
        Some(("validate", sub_args)) => {
            let path = Path::new(sub_args.value_of("PATH").unwrap());
            match parse_stream_def(path) {
                Ok(model) => {
                    let reader = RecordReader::new(&model.input.schema)?;
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
            run()?;
        }
        _ => unreachable!(
            "Exhausted list of subcommands and SubcommandRequiredElseHelp prevents `None`"
        ),
    }
    Ok(())
}

fn run() -> Result<()> {
    let config = match Config::new() {
        Ok(ok) => ok,
        Err(err) => {
            println!("{}\n{}", err, *ENVIRONMENT_HELP_STRING);
            return Ok(());
        }
    };

    configure_logging(config.log_level)?;
    log::info!(
        "staring prometheus metrics endpoint at {}",
        &config.metrics_port
    );
    prometheus_exporter::start(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        config.metrics_port,
    ))?;

    create_threads(&config)
}

fn create_threads(config: &Config) -> Result<()> {
    let mut w = ThreadWaiter::new();
    let (tx, rx) = create_queue(config.queue_capacity);
    let (tx_reject, rx_reject) = std::sync::mpsc::channel();

    let (stream_reader_data, stream_writer_data) =
        configure_streams(&config.stream_dir, &config.root_dir)?;
    let stream_reject_data = configure_reject_db(&config.reject_db, &stream_writer_data)?;

    for i in stream_writer_data.iter() {
        STREAMS.with_label_values(&[&i.name]).set(1);
    }

    create_reject_handle(&mut w, rx_reject, &config.reject_db, &stream_reject_data)?;
    create_replay_handles(
        &mut w,
        config.kafka.config(),
        config.schema_registry.config(),
        &config.reject_db,
        &tx_reject,
        stream_writer_data.clone(),
    )?;
    create_workers(
        &mut w,
        config.kafka.config(),
        config.schema_registry.config(),
        rx,
        &tx_reject,
        stream_writer_data,
        config.workers,
    )?;
    let _watch = create_fswatch(&mut w, tx, stream_reader_data, Duration::from_secs(2))?;

    w.wait_for()
}
