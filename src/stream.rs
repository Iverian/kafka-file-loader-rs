use std::path::{Path, PathBuf};

use regex::Regex;
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};
use stable_eyre::eyre::{bail, Result};

use crate::{
    csv::record_reader::RecordReader,
    model::{check_stream_def_path, parse_stream_def},
};

lazy_static! {
    static ref STREAM_DEF_PATTERN: Regex = Regex::new(r"^[a-zA-Z0-9._-]+\.ya?ml$").unwrap();
    static ref STREAM_REPLACE: Regex = Regex::new(r"[.-_]").unwrap();
}

#[derive(Clone, Debug)]
pub struct StreamWriterData {
    pub name: String,
    pub topic: String,
    pub sr_strategy: SubjectNameStrategy,
    pub reader: RecordReader,
}

#[derive(Clone, Debug)]
pub struct StreamRejectData {
    pub name: String,
}

#[derive(Clone, Debug)]
pub struct StreamReaderData {
    pub data_dir: PathBuf,
    pub pattern: Regex,
}

struct StreamDef(StreamReaderData, StreamWriterData, StreamRejectData);

pub async fn configure_streams(
    stream_dir: &Path,
    root_dir: &Path,
) -> Result<(
    Vec<StreamReaderData>,
    Vec<StreamWriterData>,
    Vec<StreamRejectData>,
)> {
    let mut rd = tokio::fs::read_dir(stream_dir).await?;

    let mut reader = Vec::new();
    let mut writer = Vec::new();
    let mut reject = Vec::new();

    while let Some(entry) = rd.next_entry().await? {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if check_stream_def_path(&entry.path()).is_err() {
            continue;
        }

        match configure_stream(entry.path(), &name_str, root_dir).await {
            Ok(def) => {
                log::info!(
                    "configured stream {:?} with dir = {:?}",
                    &name_str,
                    &def.0.data_dir
                );
                reader.push(def.0);
                writer.push(def.1);
                reject.push(def.2);
            }
            Err(err) => {
                log::warn!("unable to configure stream {:?}: {}", entry.path(), err,);
            }
        }
    }

    Ok((reader, writer, reject))
}

async fn configure_stream(
    stream_def: PathBuf,
    stream_name: &str,
    root_dir: &Path,
) -> Result<StreamDef> {
    let model = parse_stream_def(stream_def).await?;
    let path = root_dir.join(model.input.dir).canonicalize()?;
    if !path.is_dir() {
        bail!("stream path is not a directory: {:?}", path);
    }
    if !path.starts_with(root_dir) {
        bail!("stream path is not under root directory");
    }
    let topic = model.output.topic;
    let reader = RecordReader::new(model.input.schema)?;

    let schema_name = model
        .output
        .value_schema_name
        .unwrap_or_else(|| format!("{}-value", &topic));
    let schema_str = reader.schema_str(&schema_name);
    log::debug!(
        "message schema for stream {:?}: {}",
        &stream_name,
        &schema_str
    );
    let schema = Box::new(SuppliedSchema {
        name: Some(schema_name),
        schema_type: SchemaType::Avro,
        schema: schema_str,
        references: Vec::new(),
    });
    let sr_strategy = SubjectNameStrategy::TopicNameStrategyWithSchema(topic.clone(), true, schema);
    let name = escape_stream_name(stream_name);

    Ok(StreamDef(
        StreamReaderData {
            data_dir: path,
            pattern: Regex::new(&model.input.pattern)?,
        },
        StreamWriterData {
            name: name.clone(),
            sr_strategy,
            topic,
            reader,
        },
        StreamRejectData { name },
    ))
}

fn escape_stream_name(name: &str) -> String {
    format!("s_{}", STREAM_REPLACE.replace_all(name, "_").to_string())
}
