use std::{
    io::stdout, path::Path, path::PathBuf, sync::mpsc::Receiver, sync::mpsc::Sender,
    sync::mpsc::TrySendError, thread, time::Duration,
};

use chrono::Local;
use chrono::{DateTime, Utc};
use eyre::bail;
use eyre::eyre;
use eyre::Result;
use fern::Dispatch;
use log::LevelFilter;
use multiqueue::{MPMCReceiver, MPMCSender};
use rdkafka::message::OwnedHeaders;
use serde::{Deserialize, Serialize};

use crate::metrics::QUEUE_SIZE;

const SEND_RETRIES: usize = 10;
const SEND_TIMEOUT: Duration = Duration::from_millis(500);

pub type ResultSender = Sender<Result<()>>;
pub type ResultReceiver = Receiver<Result<()>>;
pub type RejectItem = Box<RawStreamMessage>;
pub type RejectSender = Sender<RejectItem>;
pub type RejectReceiver = Receiver<RejectItem>;

pub struct ThreadWaiter {
    tx: ResultSender,
    rx: ResultReceiver,
    counter: usize,
}

#[derive(Clone)]
pub struct QueueSender {
    pub inner: MPMCSender<FileMessage>,
}

#[derive(Clone)]
pub struct QueueReceiver {
    pub inner: MPMCReceiver<FileMessage>,
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

#[derive(Clone, Debug)]
pub struct RawMessage {
    pub raw: String,
    pub headers: Headers,
}

impl ThreadWaiter {
    pub fn new() -> Self {
        let (tx, rx) = std::sync::mpsc::channel();
        Self { tx, rx, counter: 0 }
    }

    pub fn wait_for(&self) -> Result<()> {
        for _ in 0..self.counter {
            self.rx.recv()??;
        }
        Ok(())
    }
    pub fn spawn<F: 'static + Send + FnOnce() -> Result<()>>(&mut self, callable: F) {
        let tx = self.tx.clone();
        self.counter += 1;
        std::thread::spawn(move || {
            tx.send(callable()).unwrap();
        });
    }
}

impl QueueReceiver {
    pub fn recv(&self) -> Result<FileMessage> {
        let msg = self.inner.recv()?;
        QUEUE_SIZE.dec();
        Ok(msg)
    }
}

impl QueueSender {
    pub fn try_send(&self, mut msg: FileMessage) -> Result<()> {
        let mut retries = 0;
        loop {
            if let Err(err) = self.inner.try_send(msg) {
                match err {
                    TrySendError::Full(inner) => {
                        if retries == SEND_RETRIES {
                            bail!("error sending message: queue is full");
                        }
                        retries += 1;
                        msg = inner;
                        thread::sleep(SEND_TIMEOUT);
                    }
                    TrySendError::Disconnected(_) => {
                        bail!("error sending message: disconnected");
                    }
                }
            } else {
                QUEUE_SIZE.inc();
                break;
            }
        }
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

pub fn create_queue(capacity: u64) -> (QueueSender, QueueReceiver) {
    let (tx, rx) = multiqueue::mpmc_queue(capacity);
    (QueueSender { inner: tx }, QueueReceiver { inner: rx })
}

pub fn configure_logging(log_level: LevelFilter) -> Result<()> {
    Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}] {}",
                Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        .level(log_level)
        .chain(stdout())
        .apply()?;
    Ok(())
}

pub fn try_send<T>(sender: &Sender<T>, mut msg: T) -> Result<()> {
    let mut retries = 0;
    loop {
        if let Err(err) = sender.send(msg) {
            if retries == SEND_RETRIES {
                bail!("error sending message");
            }
            retries += 1;
            msg = err.0;
            thread::sleep(SEND_TIMEOUT);
        } else {
            break;
        }
    }
    Ok(())
}
