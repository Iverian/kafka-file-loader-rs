use std::future::Future;
use std::mem;
use std::time::Duration;
use std::{path::Path, path::PathBuf};

use chrono::{DateTime, Utc};
use futures::future;
use rdkafka::message::OwnedHeaders;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::schema_registry_common::SubjectNameStrategy;
use serde::{Deserialize, Serialize};
use stable_eyre::eyre::Context;
use stable_eyre::eyre::Result;
use stable_eyre::eyre::{bail, eyre};
use stable_eyre::Report;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinError;

use crate::constants::{
    BASE_SEND_TIMEOUT, BROADCAST_CAPACITY, MAX_SEND_TIMEOUT, SEND_TIMEOUT_MULTIPLIER,
};
use crate::csv::record_reader::Record;
use crate::csv::util::ReadItem;
use crate::metrics::QUEUE_SIZE;

pub type RejectItem = Box<RawStreamMessage>;
pub type RejectSender = AsyncChannelSender<RejectItem>;
pub type RejectReceiver = AsyncChannelReceiver<RejectItem>;
pub type TaskJoinHandle = tokio::task::JoinHandle<TaskExitStatus>;
pub type CancelSender = tokio::sync::broadcast::Sender<()>;
pub type CancelReceiver = tokio::sync::broadcast::Receiver<()>;

#[derive(Clone)]
pub struct QueueSender {
    inner: flume::Sender<FileMessage>,
}

#[derive(Clone)]
pub struct QueueReceiver {
    pub inner: flume::Receiver<FileMessage>,
}

#[derive(Clone)]
pub struct ChannelSender<T> {
    pub inner: std::sync::mpsc::Sender<T>,
}

pub struct ChannelReceiver<T> {
    pub inner: std::sync::mpsc::Receiver<T>,
}

#[derive(Clone)]
pub struct AsyncChannelSender<T> {
    pub inner: tokio::sync::mpsc::Sender<T>,
}

pub struct AsyncChannelReceiver<T> {
    pub inner: tokio::sync::mpsc::Receiver<T>,
}

#[derive(Clone, Debug)]
pub struct FileMessage {
    pub id: usize,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Headers {
    pub filename: String,
    pub ctime: DateTime<Utc>,
    pub mtime: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct RawStreamMessage {
    pub name: String,
    pub payload: RawMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RawMessage {
    pub raw: String,
    pub headers: Headers,
}

#[derive(Debug)]
pub struct TaskHandle {
    inner: Vec<TaskJoinHandle>,
    tx: CancelSender,
}

pub struct CancelToken {
    rx: CancelReceiver,
}

#[derive(Debug)]
pub enum TaskExitStatus {
    Exited,
    Completed,
    Err(Report),
}
impl CancelToken {
    pub async fn recv(&mut self) -> Result<()> {
        self.rx
            .recv()
            .await
            .map_err(|err| eyre!("error receiving cancellation: {}", err))
    }
}

impl TaskHandle {
    pub fn new() -> Self {
        let (tx, _) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: Vec::new(),
            tx,
        }
    }

    pub fn cancel_token(&self) -> CancelToken {
        CancelToken {
            rx: self.tx.subscribe(),
        }
    }

    pub fn push_job<F: Future<Output = Result<()>> + Send + 'static>(&mut self, future: F) {
        self.inner.push(tokio::spawn(async move {
            match future.await {
                Ok(_) => TaskExitStatus::Completed,
                Err(err) => TaskExitStatus::Err(err),
            }
        }));
    }

    pub fn push_task<F: Future<Output = Result<()>> + Send + 'static>(&mut self, future: F) {
        self.inner.push(tokio::spawn(async move {
            match future.await {
                Ok(_) => TaskExitStatus::Exited,
                Err(err) => TaskExitStatus::Err(err),
            }
        }));
    }

    pub async fn join(mut self) -> Result<()> {
        let cancel_fut = tokio::spawn(async move {
            match tokio::signal::ctrl_c().await {
                Ok(_) => TaskExitStatus::Exited,
                Err(err) => TaskExitStatus::Err(Report::new(err)),
            }
        });
        self.inner.push(cancel_fut);

        let mut futures = mem::replace(&mut self.inner, Vec::new());
        log::info!("waiting for {} tasks", futures.len());
        loop {
            if let Some(futs) = self.complete(future::select_all(futures).await).await? {
                log::info!("{} tasks left", futs.len());
                futures = futs;
            } else {
                break;
            }
        }
        Ok(())
    }

    async fn complete(
        &self,
        select_all: (
            Result<TaskExitStatus, JoinError>,
            usize,
            Vec<TaskJoinHandle>,
        ),
    ) -> Result<Option<Vec<TaskJoinHandle>>> {
        let (result, _, futures) = select_all;
        match result {
            Ok(res) => match res {
                TaskExitStatus::Completed => {
                    log::info!("task completed");
                }
                TaskExitStatus::Exited => {
                    log::info!("task exited");
                    self.tx.send(())?;
                    TaskHandle::cancel(futures).await;
                    return Ok(None);
                }
                TaskExitStatus::Err(err) => {
                    log::warn!("task failed: {}", err);
                    self.tx.send(())?;
                    TaskHandle::cancel(futures).await;
                    return Ok(None);
                }
            },
            Err(err) => {
                log::warn!("error joining task: {}", err);
                return Ok(Some(futures));
            }
        }
        Ok(if futures.is_empty() {
            None
        } else {
            Some(futures)
        })
    }

    async fn cancel(futures: Vec<TaskJoinHandle>) {
        log::info!("cancelling running tasks");
        for fut in futures.into_iter() {
            TaskHandle::log_task_error(fut.await);
        }
    }

    fn log_task_error(task_result: Result<TaskExitStatus, JoinError>) {
        if let Ok(res) = task_result {
            if let TaskExitStatus::Err(err) = res {
                log::warn!("task failed: {}", err);
            }
        }
    }
}

impl<T> ChannelReceiver<T> {
    pub fn try_recv(&self) -> Result<T, std::sync::mpsc::TryRecvError> {
        self.inner.try_recv()
    }
}

impl<T> AsyncChannelSender<T> {
    pub async fn send_async(&self, mut message: T) -> Result<()> {
        let mut timeout = BASE_SEND_TIMEOUT;
        loop {
            if let Err(err) = self.inner.try_send(message) {
                match err {
                    TrySendError::Full(msg) => {
                        timeout = wait_for_retry_async(timeout).await;
                        message = msg;
                    }
                    TrySendError::Closed(_) => {
                        bail!("channel disconnected");
                    }
                }
            } else {
                return Ok(());
            }
        }
    }

    pub fn send(&self, mut message: T) -> Result<()> {
        let mut timeout = BASE_SEND_TIMEOUT;
        loop {
            if let Err(err) = self.inner.try_send(message) {
                match err {
                    TrySendError::Full(msg) => {
                        timeout = wait_for_retry(timeout);
                        message = msg;
                    }
                    TrySendError::Closed(_) => {
                        bail!("channel disconnected");
                    }
                }
            } else {
                return Ok(());
            }
        }
    }
}

impl<T> AsyncChannelReceiver<T> {
    pub async fn recv(&mut self) -> Result<T> {
        self.inner
            .recv()
            .await
            .ok_or_else(|| eyre!("channel closed"))
    }
}

impl QueueReceiver {
    pub async fn recv(&self) -> Result<FileMessage> {
        let msg = self.inner.recv_async().await?;
        QUEUE_SIZE.dec();
        Ok(msg)
    }
}

impl QueueSender {
    pub async fn send(&self, msg: FileMessage) -> Result<()> {
        self.inner.send_async(msg).await?;
        QUEUE_SIZE.inc();
        Ok(())
    }
}

impl RawStreamMessage {
    pub fn new(name: String, raw: String, headers: Headers) -> Self {
        Self {
            name,
            payload: RawMessage { raw, headers },
        }
    }

    pub fn kafka_headers(&self) -> OwnedHeaders {
        self.payload.headers.to_kafka_headers()
    }
}

impl Headers {
    pub fn from_path(path: &Path) -> Result<Headers> {
        let metadata = path.metadata()?;

        let ctime: DateTime<Utc> = metadata.created()?.into();
        let mtime: DateTime<Utc> = metadata.modified()?.into();
        Ok(Headers {
            filename: path
                .file_name()
                .ok_or_else(|| eyre!("no file name for path: {:?}", path))?
                .to_str()
                .ok_or_else(|| eyre!("unable to convert os string to unicode"))?
                .to_owned(),
            mtime,
            ctime,
        })
    }

    pub fn to_kafka_headers(&self) -> OwnedHeaders {
        OwnedHeaders::new_with_capacity(3)
            .add("filename", &self.filename)
            .add("mtime", &self.mtime.to_rfc3339())
            .add("ctime", &self.ctime.to_rfc3339())
    }
}

pub fn create_sync_channel<T>() -> (ChannelSender<T>, ChannelReceiver<T>) {
    let (tx, rx) = std::sync::mpsc::channel::<T>();
    (ChannelSender { inner: tx }, ChannelReceiver { inner: rx })
}

pub fn create_async_channel<T>(
    capacity: usize,
) -> (AsyncChannelSender<T>, AsyncChannelReceiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(capacity);
    (
        AsyncChannelSender { inner: tx },
        AsyncChannelReceiver { inner: rx },
    )
}

pub fn create_queue(capacity: usize) -> (QueueSender, QueueReceiver) {
    let (tx, rx) = flume::bounded(capacity);
    (QueueSender { inner: tx }, QueueReceiver { inner: rx })
}

pub async fn encode_read_item(
    avro_encoder: &AvroEncoder<'_>,
    item: ReadItem<Record<'_>>,
    sr_strategy: SubjectNameStrategy,
) -> ReadItem<Vec<u8>> {
    ReadItem {
        raw: item.raw,
        result: match item.result {
            Ok(value) => avro_encoder
                .encode(value, sr_strategy)
                .await
                .wrap_err("unable to encode message to avro"),
            Err(err) => Err(err),
        },
    }
}

pub fn wait_for_retry(timeout: Duration) -> Duration {
    std::thread::sleep(timeout);
    std::cmp::min(MAX_SEND_TIMEOUT, timeout * SEND_TIMEOUT_MULTIPLIER)
}

pub async fn wait_for_retry_async(timeout: Duration) -> Duration {
    tokio::time::sleep(timeout).await;
    std::cmp::min(MAX_SEND_TIMEOUT, timeout * SEND_TIMEOUT_MULTIPLIER)
}
