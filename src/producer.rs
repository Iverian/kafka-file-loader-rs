use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use eyre::eyre;
use eyre::Report;
use eyre::Result;
use rdkafka::producer::BaseRecord;
use rdkafka::producer::ProducerContext;
use rdkafka::producer::ThreadedProducer;
use rdkafka::ClientConfig;
use rdkafka::ClientContext;
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;

use crate::metrics::RECORDS;
use crate::util::try_send;
use crate::util::Headers;
use crate::util::RawStreamMessage;
use crate::util::RejectItem;
use crate::{
    csv::{record_reader::Record, util::ReadItem},
    util::RejectSender,
};

pub type KafkaProducer = ThreadedProducer<RejectContext>;

pub struct ProduceHandle {
    producer: ThreadedProducer<RejectContext>,
    encoder: AvroEncoder,
    tx_reject: RejectSender,
}

#[derive(Clone, Debug)]
pub struct RejectContext {
    tx_reject: Arc<Mutex<RejectSender>>,
}

impl ProduceHandle {
    pub fn new(
        config: &ClientConfig,
        encoder: AvroEncoder,
        tx_reject: RejectSender,
    ) -> Result<Self> {
        Ok(Self {
            producer: create_kafka_producer(config, tx_reject.clone())?,
            encoder,
            tx_reject,
        })
    }

    pub fn send(
        &mut self,
        topic: &str,
        strategy: &SubjectNameStrategy,
        headers: Headers,
        name: String,
        item: ReadItem<Record>,
    ) -> Result<()> {
        let raw = item.raw.ok_or_else(|| eyre!("error reading line"))?;
        self.send_inner(
            topic,
            strategy,
            Box::new(RawStreamMessage::new(name, raw, headers)),
            item.result,
        )?;
        Ok(())
    }

    fn send_inner(
        &mut self,
        topic: &str,
        strategy: &SubjectNameStrategy,
        msg: RejectItem,
        parsed: Result<Record>,
    ) -> Result<()> {
        if let Err((err, msg)) = self.try_send_inner(topic, strategy, msg, parsed) {
            reject_message(&self.tx_reject, err, msg)?;
        }
        Ok(())
    }

    fn try_send_inner(
        &mut self,
        topic: &str,
        strategy: &SubjectNameStrategy,
        msg: RejectItem,
        parsed: Result<Record>,
    ) -> Result<(), (Report, RejectItem)> {
        match self.try_encode(strategy, parsed) {
            Ok(rec) => {
                let headers = msg.kafka_headers();
                self.producer
                    .send(
                        BaseRecord::with_opaque_to(topic, msg)
                            .headers(headers)
                            .key(&vec![])
                            .payload(&rec),
                    )
                    .map_err(|(err, rec)| (Report::new(err), rec.delivery_opaque))
            }
            Err(err) => Err((err, msg)),
        }
    }

    fn try_encode(
        &mut self,
        strategy: &SubjectNameStrategy,
        parsed: Result<Record>,
    ) -> Result<Vec<u8>> {
        let result = self.encoder.encode_struct(parsed?, strategy)?;
        Ok(result)
    }
}

impl RejectContext {
    pub fn new(tx_reject: RejectSender) -> Self {
        Self {
            tx_reject: Arc::new(Mutex::new(tx_reject)),
        }
    }

    fn reject(
        &self,
        err: Report,
        delivery_opaque: <RejectContext as ProducerContext>::DeliveryOpaque,
    ) -> Result<()> {
        reject_message(&*self.lock()?, err, delivery_opaque)
    }

    fn lock(&self) -> Result<MutexGuard<RejectSender>> {
        self.tx_reject
            .lock()
            .map_err(|err| eyre!("error acquiring lock: {}", err))
    }
}

impl ClientContext for RejectContext {}

impl ProducerContext for RejectContext {
    type DeliveryOpaque = RejectItem;

    fn delivery(
        &self,
        delivery_result: &rdkafka::producer::DeliveryResult<'_>,
        delivery_opaque: Self::DeliveryOpaque,
    ) {
        match delivery_result {
            Ok(_) => {
                RECORDS
                    .with_label_values(&[&delivery_opaque.name, "sent"])
                    .inc();
            }
            Err((err, _)) => {
                if let Err(err) = self.reject(Report::new(err.clone()), delivery_opaque.clone()) {
                    log::warn!(
                        "error rejecting message (err = {}, raw = {})",
                        err,
                        &delivery_opaque.payload.raw
                    );
                }
            }
        }
    }
}

fn reject_message(tx: &RejectSender, err: Report, msg: RejectItem) -> Result<()> {
    log::warn!(
        "error sending message (err = {}, raw = {})",
        err,
        &msg.payload.raw
    );
    RECORDS.with_label_values(&[&msg.name, "rejected"]).inc();
    try_send(tx, msg)?;
    Ok(())
}

pub fn create_kafka_producer(
    config: &ClientConfig,
    tx_reject: RejectSender,
) -> Result<KafkaProducer> {
    let result = config.create_with_context(RejectContext::new(tx_reject))?;
    Ok(result)
}
