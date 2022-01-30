use std::sync::mpsc::Receiver;
use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use eyre::Result;
use eyre::{bail, eyre};
use notify::DebouncedEvent;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use regex::Regex;

use crate::stream::StreamReaderData;
use crate::util::FileMessage;
use crate::util::QueueSender;
use crate::util::ThreadWaiter;

#[derive(Debug, Clone)]
struct WatchItem {
    pub id: usize,
    pub pattern: Regex,
}

type PatternsMap = Arc<HashMap<PathBuf, Vec<WatchItem>>>;

struct WatchLoopImpl {
    pub tx: QueueSender,
    pub patterns: PatternsMap,
    pub rx: Receiver<DebouncedEvent>,
}

pub fn create_fswatch(
    waiter: &mut ThreadWaiter,
    tx: QueueSender,
    def: Vec<StreamReaderData>,
    delay: Duration,
) -> Result<RecommendedWatcher> {
    let (tx_sync, rx_sync) = std::sync::mpsc::channel();
    let mut watch: RecommendedWatcher = Watcher::new(tx_sync, delay)?;

    initialize_watch(&mut watch, &def)?;
    initialize_readers(waiter, &tx, &def)?;
    WatchLoopImpl::spawn(waiter, rx_sync, &tx, create_watch_map(def));

    Ok(watch)
}

fn initialize_watch(watch: &mut RecommendedWatcher, def: &Vec<StreamReaderData>) -> Result<()> {
    for i in def.iter() {
        watch.watch(&i.data_dir, RecursiveMode::NonRecursive)?;
    }
    Ok(())
}

fn initialize_readers(
    waiter: &mut ThreadWaiter,
    tx: &QueueSender,
    def: &Vec<StreamReaderData>,
) -> Result<()> {
    for (idx, entry) in def.iter().enumerate() {
        let entry = entry.clone();
        let tx = tx.clone();
        waiter.spawn(move || initialize_single_reader(tx, idx, entry));
    }
    Ok(())
}

fn initialize_single_reader(tx: QueueSender, id: usize, def: StreamReaderData) -> Result<()> {
    for i in std::fs::read_dir(&def.data_dir)? {
        let entry = i?;
        if entry.file_type()?.is_file()
            && def.pattern.is_match(&entry.file_name().to_string_lossy())
        {
            tx.try_send(FileMessage {
                id,
                path: entry.path().canonicalize()?,
            })?;
        }
    }
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

impl WatchLoopImpl {
    fn spawn(
        waiter: &mut ThreadWaiter,
        rx: Receiver<DebouncedEvent>,
        tx: &QueueSender,
        patterns: PatternsMap,
    ) -> usize {
        let tx = tx.clone();
        waiter.spawn(move || {
            let mut w = WatchLoopImpl { tx, patterns, rx };
            w.match_loop()
        });
        1
    }

    fn match_loop(&mut self) -> Result<()> {
        loop {
            self.match_event(self.rx.recv()?)?;
        }
    }

    fn match_event(&self, event: DebouncedEvent) -> Result<()> {
        match event {
            DebouncedEvent::Create(path) => {
                if !path.is_file() {
                    return Ok(());
                }
                self.match_file(path.canonicalize()?)?;
            }
            DebouncedEvent::Error(err, _) => {
                bail!("error processing fs events: {}", err);
            }
            _ => {}
        }
        Ok(())
    }

    fn match_file(&self, path: PathBuf) -> Result<()> {
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
                    self.tx.try_send(FileMessage {
                        id: watch.id,
                        path: path.clone(),
                    })?;
                    break;
                }
            }
        }
        Ok(())
    }
}
