use std::path::{Path, PathBuf};

use eyre::{bail, Result};
use regex::Regex;
use schema_registry_converter::schema_registry_common::{
    SchemaType, SubjectNameStrategy, SuppliedSchema,
};

use crate::{csv::record_reader::RecordReader, model::parse_stream_def};

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
pub struct StreamReaderData {
    pub data_dir: PathBuf,
    pub pattern: Regex,
}

struct StreamDef(StreamReaderData, StreamWriterData);

pub fn configure_streams(
    stream_dir: &Path,
    root_dir: &Path,
) -> Result<(Vec<StreamReaderData>, Vec<StreamWriterData>)> {
    let mut reader = Vec::new();
    let mut writer = Vec::new();

    for i in std::fs::read_dir(stream_dir)? {
        let entry = i?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();

        if entry.file_type()?.is_file() && STREAM_DEF_PATTERN.is_match(&name_str) {
            match configure_stream(&entry.path(), &name_str, root_dir) {
                Ok(def) => {
                    reader.push(def.0);
                    writer.push(def.1);
                }
                Err(err) => {
                    log::warn!("unable to add stream {:?}: {}", entry.path(), err);
                }
            }
        }
    }

    Ok((reader, writer))
}

fn configure_stream(stream_def: &Path, stream_name: &str, root_dir: &Path) -> Result<StreamDef> {
    let model = parse_stream_def(stream_def)?;
    let path = root_dir.join(model.input.dir).canonicalize()?;
    if !path.is_dir() {
        bail!("stream path is not a directory: {:?}", path);
    }
    if !path.starts_with(root_dir) {
        bail!("stream path is not under root directory");
    }
    let topic = model.output.topic.as_str();
    let reader = RecordReader::new(&model.input.schema)?;
    let schema = Box::new(SuppliedSchema {
        name: Some(reader.schema_name(topic)),
        schema_type: SchemaType::Avro,
        schema: reader.schema_str(topic),
        references: Vec::new(),
    });
    let sr_strategy =
        SubjectNameStrategy::TopicNameStrategyWithSchema(model.output.topic.clone(), true, schema);

    Ok(StreamDef(
        StreamReaderData {
            data_dir: path,
            pattern: Regex::new(&model.input.pattern)?,
        },
        StreamWriterData {
            name: escape_stream_name(stream_name),
            sr_strategy,
            topic: model.output.topic,
            reader,
        },
    ))
}

fn escape_stream_name(name: &str) -> String {
    format!("s_{}", STREAM_REPLACE.replace_all(name, "_").to_string())
}
