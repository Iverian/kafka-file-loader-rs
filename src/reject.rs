use std::collections::HashMap;
use std::path::{Path, PathBuf};

use eyre::Result;
use rdkafka::ClientConfig;
use rusqlite::{params, Connection, OpenFlags};
use schema_registry_converter::blocking::avro::AvroEncoder;
use schema_registry_converter::blocking::schema_registry::SrSettings;

use crate::producer::ProduceHandle;
use crate::{
    stream::StreamWriterData,
    util::ThreadWaiter,
    util::{RawMessage, RejectReceiver, RejectSender},
};

#[derive(Debug, Clone)]
pub struct StreamRejectData {
    name: String,
    query: String,
}

pub fn configure_reject_db(
    reject_db: &Path,
    stream_writer_data: &Vec<StreamWriterData>,
) -> Result<Vec<StreamRejectData>> {
    let mut result = Vec::new();
    let conn = sqlite_open(reject_db)?;
    conn.execute("PRAGMA auto_vacuum = FULL", [])?;
    conn.execute("VACUUM", [])?;
    for i in stream_writer_data.iter() {
        let q = format!(
            "CREATE TABLE IF NOT EXISTS {} (
                id INTEGER PRIMARY KEY,
                headers BLOB,
                raw BLOB
            )",
            &i.name
        );
        conn.execute(&q, [])?;
        result.push(StreamRejectData {
            name: i.name.to_owned(),
            query: format!("INSERT INTO {} (headers, raw) VALUES (?1, ?2)", &i.name),
        });
    }
    Ok(result)
}

pub fn create_replay_handles(
    waiter: &mut ThreadWaiter,
    kafka_config: &ClientConfig,
    sr_config: &SrSettings,
    reject_db: &Path,
    tx_reject: &RejectSender,
    def: Vec<StreamWriterData>,
) -> Result<()> {
    for i in def.into_iter() {
        let tx_reject = tx_reject.clone();
        let reject_db = reject_db.to_owned();
        let producer = ProduceHandle::new(
            kafka_config,
            AvroEncoder::new(sr_config.clone()),
            tx_reject.clone(),
        )?;
        waiter.spawn(move || replay_handle(reject_db, producer, i));
    }
    Ok(())
}

fn replay_handle(
    reject_db: PathBuf,
    mut producer: ProduceHandle,
    def: StreamWriterData,
) -> Result<()> {
    let conn = sqlite_open(&reject_db)?;
    let id = get_max_stream_id(&conn, &def.name)?;

    let q = format!("SELECT headers, raw FROM {} WHERE id <= ?1", &def.name);
    let mut st = conn.prepare(&q)?;

    for i in st.query_map(params![id], |r| {
        Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
    })? {
        let i = i?;
        let msg = RawMessage {
            headers: serde_json::from_slice(&i.0)?,
            raw: serde_json::from_slice(&i.1)?,
        };
        producer.send(
            &def.topic,
            &def.sr_strategy,
            msg.headers,
            def.name.clone(),
            def.reader.read_owned(msg.raw),
        )?;
    }

    let q = format!("DELETE FROM {} WHERE id <= ?1", &def.name);
    let mut st = conn.prepare(&q)?;
    st.execute([])?;

    Ok(())
}

fn get_max_stream_id(conn: &Connection, name: &str) -> Result<usize> {
    let q = format!("SELECT coalesce(max(id), 0) FROM {}", name);
    let mut st = conn.prepare(&q)?;
    let result = st.query_map([], |r| Ok(r.get(0)?))?.last().unwrap()?;
    Ok(result)
}

pub fn create_reject_handle(
    waiter: &mut ThreadWaiter,
    rx: RejectReceiver,
    reject_db: &Path,
    data: &Vec<StreamRejectData>,
) -> Result<()> {
    let reject_db = reject_db.to_owned();
    let data = data.clone();
    waiter.spawn(move || reject_handle(rx, reject_db, data));
    Ok(())
}

fn reject_handle(
    rx: RejectReceiver,
    reject_db: PathBuf,
    data: Vec<StreamRejectData>,
) -> Result<()> {
    let conn = sqlite_open(&reject_db)?;
    let mut statements = HashMap::new();
    for i in data.into_iter() {
        statements.insert(i.name, conn.prepare(&i.query)?);
    }

    loop {
        let msg = rx.recv()?;
        if let Some(q) = statements.get_mut(&msg.name) {
            q.execute(params![
                serde_json::to_vec(&msg.payload.headers)?,
                serde_json::to_vec(&msg.payload.raw)?
            ])?;
        }
    }
}

fn sqlite_open(path: &Path) -> Result<Connection> {
    let conn = rusqlite::Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_NO_MUTEX | OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    Ok(conn)
}
