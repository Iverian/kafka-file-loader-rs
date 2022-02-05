use std::sync::Arc;

use stable_eyre::eyre::eyre;
use stable_eyre::eyre::Report;
use stable_eyre::eyre::Result;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::util::Timeout;
use rdkafka::ClientConfig;

use crate::metrics::RECORDS;
use crate::util::Headers;
use crate::util::RawStreamMessage;
use crate::util::RejectItem;
use crate::{csv::util::ReadItem, util::RejectSender};

pub type KafkaProducer = FutureProducer;

pub struct ProduceHandle {
    producer: Arc<KafkaProducer>,
    tx_reject: RejectSender,
}

impl ProduceHandle {
    pub fn new(config: &ClientConfig, tx_reject: RejectSender) -> Result<Self> {
        Ok(Self {
            producer: Arc::new(config.create()?),
            tx_reject,
        })
    }

    pub async fn send(
        &mut self,
        topic: &str,
        headers: Headers,
        name: String,
        item: ReadItem<Vec<u8>>,
    ) -> Result<()> {
        let raw = item.raw.ok_or_else(|| eyre!("error reading line"))?;
        self.send_inner(
            topic,
            Box::new(RawStreamMessage::new(name, raw, headers)),
            item.result,
        )
        .await?;
        Ok(())
    }

    async fn send_inner(
        &mut self,
        topic: &str,
        msg: RejectItem,
        encoded_msg: Result<Vec<u8>>,
    ) -> Result<()> {
        self.try_send_inner(topic, msg, encoded_msg).await?;
        Ok(())
    }

    async fn try_send_inner(
        &mut self,
        topic: &str,
        msg: RejectItem,
        encoded_msg: Result<Vec<u8>>,
    ) -> Result<()> {
        match encoded_msg {
            Ok(rec) => {
                let headers = msg.kafka_headers();
                let key = vec![];

                match self
                    .producer
                    .send(
                        FutureRecord::to(topic)
                            .headers(headers)
                            .key(&key)
                            .payload(&rec),
                        Timeout::Never,
                    )
                    .await
                {
                    Ok(_) => RECORDS.with_label_values(&[&msg.name, "sent"]).inc(),
                    Err((err, _)) => self.reject(Report::new(err), msg).await?,
                }
            }
            Err(err) => self.reject(err, msg).await?,
        }
        Ok(())
    }

    async fn reject(&self, err: Report, msg: RejectItem) -> Result<()> {
        log::warn!(
            "error sending message (err = {}, raw = {})",
            err,
            &msg.payload.raw
        );
        RECORDS.with_label_values(&[&msg.name, "rejected"]).inc();
        self.tx_reject.send_async(msg).await?;
        Ok(())
    }
}
