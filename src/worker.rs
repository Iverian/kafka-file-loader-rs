use std::fs::File;
use std::sync::Arc;

use eyre::Result;
use rdkafka::ClientConfig;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;

use crate::metrics::FILES;
use crate::metrics::QUEUE_SIZE;
use crate::producer::ProduceHandle;
use crate::stream::StreamWriterData;
use crate::util::Headers;
use crate::util::QueueReceiver;
use crate::util::RejectSender;
use crate::util::ThreadWaiter;

struct Worker {
    rx: QueueReceiver,
    def: Arc<Vec<StreamWriterData>>,
    producer: ProduceHandle,
}

pub fn create_workers(
    waiter: &mut ThreadWaiter,
    kafka_config: &ClientConfig,
    sr_config: &SrSettings,
    rx: QueueReceiver,
    tx_reject: &RejectSender,
    def: Vec<StreamWriterData>,
    number: usize,
) -> Result<()> {
    let shared_data = Arc::new(def);
    for _ in 0..number {
        let def = shared_data.clone();
        let rx = rx.clone();
        let producer = ProduceHandle::new(
            kafka_config,
            AvroEncoder::new(sr_config.clone()),
            tx_reject.clone(),
        )?;
        waiter.spawn(move || spawn_worker(producer, rx, def));
    }
    Ok(())
}

fn spawn_worker(
    producer: ProduceHandle,
    rx: QueueReceiver,
    def: Arc<Vec<StreamWriterData>>,
) -> Result<()> {
    Worker { rx, def, producer }.run()
}

impl Worker {
    fn run(mut self) -> Result<()> {
        loop {
            let msg = self.rx.recv()?;
            QUEUE_SIZE.dec();
            if !msg.path.exists() {
                log::warn!("path {:?} does not exist, skipping", &msg.path);
            }

            let def = &self.def[msg.id];
            let headers = Headers::from_path(&msg.path)?;
            for item in def.reader.read(File::open(&msg.path)?) {
                self.producer.send(
                    &def.topic,
                    &def.sr_strategy,
                    headers.clone(),
                    def.name.clone(),
                    item,
                )?;
            }

            FILES.with_label_values(&[&def.name]).inc();
            std::fs::remove_file(&msg.path)
                .unwrap_or_else(|err| log::warn!("unable to unlink file {:?}: {}", &msg.path, err));
        }
    }
}
