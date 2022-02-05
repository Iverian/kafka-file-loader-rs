use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use notify::DebouncedEvent;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;
use stable_eyre::eyre::Result;
use stable_eyre::eyre::{bail, eyre};
use tokio::fs::DirEntry;

use crate::constants::{RESEND_BATCH, RESEND_TIMEOUT};
use crate::stream::StreamReaderData;
use crate::util::FileMessage;
use crate::util::{
    create_async_channel, create_sync_channel, AsyncChannelReceiver, AsyncChannelSender,
    CancelToken, ChannelReceiver, ChannelSender, QueueSender, TaskHandle,
};

#[derive(Debug, Clone)]
struct WatchItem {
    pub id: usize,
    pub pattern: Regex,
}

type FsEventSender = ChannelSender<DebouncedEvent>;
type FsEventReceiver = ChannelReceiver<DebouncedEvent>;
type AsyncFsEventSender = AsyncChannelSender<DebouncedEvent>;
type AsyncFsEventReceiver = AsyncChannelReceiver<DebouncedEvent>;
type PatternsMap = Arc<HashMap<PathBuf, Vec<WatchItem>>>;

struct WatchLoopImpl {
    cancel: CancelToken,
    tx: QueueSender,
    patterns: PatternsMap,
    rx: AsyncFsEventReceiver,
}

pub async fn create_fswatch(
    tx: QueueSender,
    def: Vec<StreamReaderData>,
    delay: Duration,
    capacity: usize,
    handle: &mut TaskHandle,
) -> Result<RecommendedWatcher> {
    let (tx_sync, rx_sync) = create_sync_channel();
    let (tx_async, rx_async) = create_async_channel(capacity);
    create_resend_task(rx_sync, tx_async, handle);

    let watch = create_watch(tx_sync, delay, &def)?;
    create_initial_readers(&tx, &def, handle)?;
    create_fswatch_loop(rx_async, &tx, create_watch_map(def), handle);

    Ok(watch)
}

fn create_resend_task(
    rx_sync: FsEventReceiver,
    tx_async: AsyncFsEventSender,
    handle: &mut TaskHandle,
) {
    let cancel = handle.cancel_token();
    handle.push(tokio::spawn(async move {
        resend_task(cancel, rx_sync, tx_async).await
    }));
}

async fn resend_task(
    mut cancel: CancelToken,
    rx_sync: FsEventReceiver,
    tx_async: AsyncFsEventSender,
) -> Result<()> {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(RESEND_TIMEOUT) => {
                for _ in 0..RESEND_BATCH {
                    match rx_sync.try_recv() {
                        Ok(event) => tx_async.send(event)?,
                        Err(err) => match err {
                            std::sync::mpsc::TryRecvError::Empty => {
                                break;
                            }
                            std::sync::mpsc::TryRecvError::Disconnected => {
                                bail!("channel disconnected");
                            }
                        },
                    }
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

fn create_watch(
    tx: FsEventSender,
    delay: Duration,
    def: &Vec<StreamReaderData>,
) -> Result<RecommendedWatcher> {
    let mut watch: RecommendedWatcher = Watcher::new(tx.inner, delay)?;
    for i in def.iter() {
        watch.watch(&i.data_dir, RecursiveMode::NonRecursive)?;
    }
    Ok(watch)
}

fn create_initial_readers(
    tx: &QueueSender,
    def: &Vec<StreamReaderData>,
    handle: &mut TaskHandle,
) -> Result<()> {
    for (idx, entry) in def.iter().enumerate() {
        let entry = entry.clone();
        let tx = tx.clone();
        let cancel = handle.cancel_token();
        handle.push(tokio::spawn(async move {
            read_existing_stream_files(cancel, tx, idx, entry).await
        }));
    }
    Ok(())
}

async fn read_existing_stream_files(
    mut cancel: CancelToken,
    tx: QueueSender,
    id: usize,
    def: StreamReaderData,
) -> Result<()> {
    let mut rd = tokio::fs::read_dir(&def.data_dir).await?;

    loop {
        tokio::select! {
            res_message = rd.next_entry() => {
                if let Some(entry) = res_message? {
                    send_stream_file(entry, &tx, id, &def.pattern).await?;
                } else {
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

async fn send_stream_file(
    entry: DirEntry,
    tx: &QueueSender,
    id: usize,
    pattern: &Regex,
) -> Result<()> {
    if !entry.file_type().await?.is_file()
        || !pattern.is_match(&entry.file_name().to_string_lossy())
    {
        return Ok(());
    }
    tx.send(FileMessage {
        id,
        path: entry.path().canonicalize()?,
    })
    .await?;
    Ok(())
}

fn create_watch_map(def: Vec<StreamReaderData>) -> PatternsMap {
    let mut result = HashMap::<PathBuf, Vec<WatchItem>>::new();
    for (idx, entry) in def.into_iter().enumerate() {
        match result.get_mut(&entry.data_dir) {
            Some(watch) => {
                watch.push(WatchItem {
                    id: idx,
                    pattern: entry.pattern,
                });
            }
            None => {
                result.insert(
                    entry.data_dir,
                    vec![WatchItem {
                        id: idx,
                        pattern: entry.pattern,
                    }],
                );
            }
        }
    }
    Arc::new(result)
}

fn create_fswatch_loop(
    rx: AsyncFsEventReceiver,
    tx: &QueueSender,
    patterns: PatternsMap,
    handle: &mut TaskHandle,
) {
    let tx = tx.clone();
    let cancel = handle.cancel_token();
    handle.push(tokio::spawn(async move {
        WatchLoopImpl {
            cancel,
            tx,
            patterns,
            rx,
        }
        .match_loop()
        .await
    }));
}

impl WatchLoopImpl {
    async fn match_loop(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                res_message = self.rx.recv() => {
                    self.match_event(res_message?).await?;
                }
                res_cancel = self.cancel.recv() => {
                    res_cancel?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn match_event(&self, event: DebouncedEvent) -> Result<()> {
        match event {
            DebouncedEvent::Create(path) => {
                if !path.is_file() {
                    return Ok(());
                }
                self.match_file(path.canonicalize()?).await?;
            }
            DebouncedEvent::Error(err, _) => {
                bail!("error processing fs events: {}", err);
            }
            _ => {}
        }
        Ok(())
    }

    async fn match_file(&self, path: PathBuf) -> Result<()> {
        let name = path
            .file_name()
            .ok_or_else(|| eyre!("no file name"))?
            .to_string_lossy();
        for (dir, items) in self.patterns.iter() {
            if !path.starts_with(dir) {
                continue;
            }
            for watch in items {
                if watch.pattern.is_match(&name) {
                    self.tx
                        .send(FileMessage {
                            id: watch.id,
                            path: path.clone(),
                        })
                        .await?;
                    break;
                }
            }
        }
        Ok(())
    }
}
