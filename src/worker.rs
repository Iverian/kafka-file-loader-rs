use std::sync::Arc;

use futures::StreamExt;
use rdkafka::ClientConfig;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use stable_eyre::eyre::Result;

use crate::metrics::FILES;
use crate::producer::ProduceHandle;
use crate::stream::StreamWriterData;
use crate::util::encode_read_item;
use crate::util::CancelToken;
use crate::util::FileMessage;
use crate::util::Headers;
use crate::util::QueueReceiver;
use crate::util::RejectSender;
use crate::util::TaskHandle;
use crate::util::TaskJoinHandle;

struct Worker {
    rx: QueueReceiver,
    cancel: CancelToken,
    def: Arc<Vec<StreamWriterData>>,
    producer: ProduceHandle,
}

pub fn create_workers(
    kafka_config: &ClientConfig,
    sr_config: &SrSettings,
    rx: QueueReceiver,
    tx_reject: &RejectSender,
    def: Vec<StreamWriterData>,
    number: usize,
    handle: &mut TaskHandle,
) -> Result<()> {
    let shared_data = Arc::new(def);
    for index in 0..number {
        let def = shared_data.clone();
        let rx = rx.clone();
        let producer = ProduceHandle::new(kafka_config, tx_reject.clone())?;
        let sr_config = sr_config.clone();
        let cancel = handle.cancel_token();
        handle.push(Worker::new(cancel, index, producer, sr_config, rx, def));
    }
    Ok(())
}

impl Worker {
    fn new(
        cancel: CancelToken,
        index: usize,
        producer: ProduceHandle,
        sr_config: SrSettings,
        rx: QueueReceiver,
        def: Arc<Vec<StreamWriterData>>,
    ) -> TaskJoinHandle {
        tokio::spawn(
            async move { Worker::spawn(cancel, index, producer, sr_config, rx, def).await },
        )
    }

    async fn spawn(
        cancel: CancelToken,
        index: usize,
        producer: ProduceHandle,
        sr_config: SrSettings,
        rx: QueueReceiver,
        def: Arc<Vec<StreamWriterData>>,
    ) -> Result<()> {
        log::info!("starting worker #{}", index + 1);
        Worker {
            rx,
            cancel,
            def,
            producer,
        }
        .run(sr_config)
        .await
    }

    async fn run(mut self, sr_config: SrSettings) -> Result<()> {
        let encoder = AvroEncoder::new(sr_config);
        loop {
            tokio::select! {
                res_message = self.rx.recv() => {
                    self.send(&encoder, res_message?).await?;
                }
                res_cancel = self.cancel.recv() => {
                    res_cancel?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn send(&mut self, avro_encoder: &AvroEncoder<'_>, msg: FileMessage) -> Result<()> {
        if !msg.path.exists() {
            log::warn!("path {:?} does not exist, skipping", &msg.path);
        }

        let def = &self.def[msg.id];
        let headers = Headers::from_path(&msg.path)?;

        let mut s = def.reader.read(tokio::fs::File::open(&msg.path).await?);
        while let Some(item) = s.next().await {
            let encoded = encode_read_item(&avro_encoder, item, def.sr_strategy.clone()).await;
            self.producer
                .send(&def.topic, headers.clone(), def.name.clone(), encoded)
                .await?;
        }

        FILES.with_label_values(&[&def.name]).inc();
        tokio::fs::remove_file(&msg.path)
            .await
            .unwrap_or_else(|err| log::warn!("unable to unlink file {:?}: {}", &msg.path, err));
        Ok(())
    }
}
