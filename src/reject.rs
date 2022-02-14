use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Local;
use futures::StreamExt;
use rdkafka::ClientConfig;
use schema_registry_converter::async_impl::avro::AvroEncoder;
use schema_registry_converter::async_impl::schema_registry::SrSettings;
use stable_eyre::eyre::Result;
use stable_eyre::eyre::{ensure, eyre};
use tokio::fs::{DirEntry, File};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio_stream::wrappers::LinesStream;

use crate::producer::ProduceHandle;
use crate::stream::StreamRejectData;
use crate::util::{encode_read_item, CancelToken, RejectItem, TaskHandle};
use crate::{
    stream::StreamWriterData,
    util::{RawMessage, RejectReceiver, RejectSender},
};

const LINE_LIMIT: usize = 100000;

lazy_static! {
    static ref NEWLINE: &'static [u8] = "\n".as_bytes();
    static ref ARCHIVE_SUFFIX: &'static str = "a";
}

pub async fn create_replay_handles(
    kafka_config: &ClientConfig,
    sr_config: &SrSettings,
    reject_dir: &Path,
    tx_reject: &RejectSender,
    def: Vec<StreamWriterData>,
    handle: &mut TaskHandle,
) -> Result<()> {
    let mut writers = HashMap::new();
    for i in def.into_iter() {
        writers.insert(i.name.clone(), Arc::new(i));
    }

    let mut rd = tokio::fs::read_dir(reject_dir).await?;

    while let Some(i) = rd.next_entry().await? {
        if let Err(err) =
            try_create_replay_handle(&i, &writers, tx_reject, sr_config, kafka_config, handle).await
        {
            log::warn!("unable to replay file {:?}: {}", &i.path(), err);
            if let Err(err) = tokio::fs::remove_file(i.path()).await {
                log::warn!("unable to remove file: {}", err);
            }
        }
    }
    Ok(())
}

pub async fn create_reject_handle(
    rx: RejectReceiver,
    reject_dir: &Path,
    def: Vec<StreamRejectData>,
    handle: &mut TaskHandle,
) -> Result<()> {
    let reject_path = reject_dir.to_owned();
    let cancel = handle.cancel_token();
    handle.push_task(async move { reject_handle(cancel, rx, reject_path, def).await });
    Ok(())
}

#[derive(Debug)]
struct RejectWriterMap {
    inner: HashMap<String, RejectWriter>,
}

#[derive(Debug)]
struct RejectWriter {
    stream_name: String,
    directory: Arc<PathBuf>,
    file: RejectFileWriter,
}

#[derive(Debug)]
struct RejectFileWriter {
    file: Option<BufWriter<File>>,
    path: PathBuf,
    counter: usize,
}

impl RejectWriterMap {
    async fn new(reject_dir: PathBuf, def: Vec<StreamRejectData>) -> Result<Self> {
        let mut inner = HashMap::new();
        let path = Arc::new(reject_dir);
        for i in def.into_iter() {
            inner.insert(
                i.name.clone(),
                RejectWriter::new(i.name, path.clone()).await?,
            );
        }
        Ok(Self { inner })
    }

    fn get(&mut self, key: &str) -> Option<&mut RejectWriter> {
        self.inner.get_mut(key)
    }
}

impl Drop for RejectWriterMap {
    fn drop(&mut self) {
        log::info!("closing reject writers");
        for i in self.inner.iter_mut() {
            if let Err(err) = i.1.close() {
                log::warn!(
                    "error closing reject writer for stream {:?}: {}",
                    i.0,
                    err.to_string()
                );
            }
        }
    }
}

impl RejectWriter {
    async fn new(stream_name: String, directory: Arc<PathBuf>) -> Result<Self> {
        let file =
            RejectFileWriter::new(RejectWriter::get_file_name(&stream_name, &directory)).await?;
        Ok(Self {
            stream_name,
            directory,
            file,
        })
    }

    async fn write_line(&mut self, line: &str) -> Result<()> {
        self.file().await?.write_line(line).await?;
        Ok(())
    }

    async fn file(&mut self) -> Result<&mut RejectFileWriter> {
        if self.file.lines() >= &LINE_LIMIT {
            self.rotate_file().await?;
        }
        Ok(&mut self.file)
    }

    async fn rotate_file(&mut self) -> Result<()> {
        self.close_async().await?;
        let new_file = self.create_file().await?;
        self.file = new_file;
        Ok(())
    }

    async fn close_async(&mut self) -> Result<()> {
        self.file.close();
        let path = self.file.path_ref();
        let to = RejectWriter::get_closed_file_name(&path)?;
        tokio::fs::rename(path, to).await?;
        Ok(())
    }

    fn close(&mut self) -> Result<()> {
        self.file.close();
        let path = self.file.path_ref();
        let to = RejectWriter::get_closed_file_name(&path)?;
        std::fs::rename(path, to)?;
        Ok(())
    }

    async fn create_file(&self) -> Result<RejectFileWriter> {
        Ok(RejectFileWriter::new(RejectWriter::get_file_name(
            &self.stream_name,
            &self.directory,
        ))
        .await?)
    }

    fn get_closed_file_name(path: &Path) -> Result<PathBuf> {
        Ok(path
            .parent()
            .ok_or_else(|| eyre!("no parent in path"))?
            .join(format!(
                "{}.{}",
                path.file_name()
                    .ok_or_else(|| eyre!("no filename in path"))?
                    .to_str()
                    .ok_or_else(|| eyre!("unable to convert path to unicode"))?,
                *ARCHIVE_SUFFIX,
            )))
    }

    fn get_file_name(stream_name: &str, directory: &Path) -> PathBuf {
        directory.join(format!(
            "{}.{}.jsonl",
            stream_name,
            Local::now().format("%Y%m%dT%H%M%S")
        ))
    }
}

impl RejectFileWriter {
    async fn new(path: PathBuf) -> Result<Self> {
        log::info!("creating reject file {:?}", &path);
        let file = tokio::io::BufWriter::new(tokio::fs::File::create(&path).await?);
        Ok(Self {
            file: Some(file),
            path,
            counter: 0,
        })
    }

    async fn write_line(&mut self, line: &str) -> Result<()> {
        if let Some(inner) = self.file.as_mut() {
            inner.write(line.as_bytes()).await?;
            inner.write(&*NEWLINE).await?;
            self.counter += 1;
        }
        Ok(())
    }

    fn lines(&self) -> &usize {
        &self.counter
    }

    fn close(&mut self) {
        self.file.take();
    }

    fn path_ref(&self) -> &Path {
        &self.path
    }
}

async fn try_create_replay_handle(
    entry: &DirEntry,
    writers: &HashMap<String, Arc<StreamWriterData>>,
    tx_reject: &RejectSender,
    sr_config: &SrSettings,
    kafka_config: &ClientConfig,
    handle: &mut TaskHandle,
) -> Result<()> {
    if !entry.file_type().await?.is_file() {
        return Ok(());
    }

    let file_name = entry
        .file_name()
        .to_str()
        .ok_or_else(|| eyre!("unable to convert path to unicode"))?
        .to_string();
    let parts = file_name.split('.').collect::<Vec<_>>();
    ensure!(parts.len() == 4, "unknown filename format");
    if *parts.get(3).unwrap() != *ARCHIVE_SUFFIX {
        return Ok(());
    }

    let def = writers
        .get(*parts.get(0).unwrap())
        .ok_or_else(|| eyre!("unknown stream"))?
        .clone();

    let sr_config = sr_config.clone();
    let producer = ProduceHandle::new(kafka_config, tx_reject.clone())?;
    let path = entry.path();
    let cancel = handle.cancel_token();
    handle.push_job(async move { replay_handle(cancel, path, def, sr_config, producer).await });
    Ok(())
}

async fn replay_handle(
    mut cancel: CancelToken,
    path: PathBuf,
    def: Arc<StreamWriterData>,
    sr_config: SrSettings,
    mut producer: ProduceHandle,
) -> Result<()> {
    let encoder = AvroEncoder::new(sr_config);

    let mut lines = LinesStream::new(BufReader::new(tokio::fs::File::open(&path).await?).lines());
    log::info!("replaying file {:?}", &path);
    loop {
        tokio::select! {
            opt_line = lines.next() => {
                if let Some(res) = opt_line {
                    replay_line(&def, &encoder, &mut producer, res?).await?;
                } else {
                    log::info!("replayed file {:?}", path);
                    tokio::fs::remove_file(path).await?;
                    break;
                }
            }
            res_cancel = cancel.recv() => {
                res_cancel?;
                break;
            }
        }
    }
    Ok(())
}

async fn replay_line(
    def: &Arc<StreamWriterData>,
    encoder: &AvroEncoder<'_>,
    producer: &mut ProduceHandle,
    line: String,
) -> Result<()> {
    let msg: RawMessage = serde_json::from_slice(line.as_bytes())?;
    let item = def.reader.read_owned(msg.raw);
    let encoded = encode_read_item(encoder, item, def.sr_strategy.clone()).await;
    producer
        .send(&def.topic, msg.headers, def.name.clone(), encoded)
        .await?;
    Ok(())
}

async fn reject_handle(
    cancel: CancelToken,
    rx: RejectReceiver,
    reject_dir: PathBuf,
    def: Vec<StreamRejectData>,
) -> Result<()> {
    log::info!("writing rejected records to {:?}", &reject_dir);
    let mut writers = RejectWriterMap::new(reject_dir, def).await?;
    try_reject_handle(cancel, rx, &mut writers).await?;
    Ok(())
}

async fn try_reject_handle(
    mut cancel: CancelToken,
    mut rx: RejectReceiver,
    writers: &mut RejectWriterMap,
) -> Result<()> {
    loop {
        tokio::select! {
            res_message = rx.recv() => {
                try_reject_message(res_message?, writers).await?;
            }
            res_cancel = cancel.recv() => {
                res_cancel?;
                break;
            }
        }
    }
    Ok(())
}

async fn try_reject_message(msg: RejectItem, writers: &mut RejectWriterMap) -> Result<()> {
    if let Some(writer) = writers.get(&msg.name) {
        writer
            .write_line(&serde_json::to_string(&msg.payload)?)
            .await?;
    } else {
        log::warn!("dropping rejected message from stream {}", msg.name);
    }
    Ok(())
}
