use std::collections::HashMap;
use std::io::Read;
use std::iter::Iterator;

use eyre::ensure;
use eyre::eyre;
use eyre::Result;
use serde_json::json;
use serde_json::Value;

use super::field::Field;
use super::field_type::CsvFieldItem;
use super::reader::ReadIterator;
use super::reader::Reader;
use super::util::ReadItem;
use crate::model::{StreamFieldModel, StreamSchemaModel};

#[derive(Clone, Debug)]
pub struct RecordReader {
    fields: Vec<Field>,
    reader: Reader,
    null_string: String,
}

pub struct RecordReadIterator<'a, R: Read> {
    parent: &'a RecordReader,
    lines: ReadIterator<'a, R>,
}

pub type Record = HashMap<String, CsvFieldItem>;

impl RecordReader {
    pub fn new(model: &StreamSchemaModel) -> Result<Self> {
        Ok(Self {
            fields: RecordReader::fields(&model.fields)?,
            reader: Reader::new(
                model.format.header,
                model.format.delimiter,
                model.format.escape,
            ),
            null_string: model.format.null_string.clone(),
        })
    }

    pub fn read<R: Read>(&self, read: R) -> RecordReadIterator<R> {
        RecordReadIterator {
            parent: &self,
            lines: self.reader.read(read),
        }
    }

    pub fn read_parsed(&self, line: Vec<String>) -> Result<Record> {
        let mut result = HashMap::new();
        ensure!(line.len() <= self.fields.len(), "too many records");

        for (idx, field) in self.fields.iter().enumerate() {
            let is_null = idx < line.len() && line[idx] != self.null_string;
            ensure!(
                !field.optional() && !is_null,
                format!("required field '{}' is missing", field.name())
            );

            result.insert(
                field.name().to_owned(),
                field
                    .cast(if is_null {
                        Some(line[idx].as_str())
                    } else {
                        None
                    })
                    .map_err(|err| eyre!("error parsing field '{}': {}", field.name(), err))?,
            );
        }
        Ok(result)
    }

    pub fn read_owned(&self, line: String) -> ReadItem<Record> {
        match self.reader.read_line(line.as_bytes()) {
            Ok(rec) => ReadItem {
                raw: Some(line),
                result: self.read_parsed(rec),
            },
            Err(err) => ReadItem {
                raw: Some(line),
                result: Err(err),
            },
        }
    }

    pub fn schema_name(&self, topic: &str) -> String {
        format!("{}-value", topic)
    }

    pub fn schema_str(&self, topic: &str) -> String {
        json!({
            "type": "record",
            "name": self.schema_name(topic),
            "fields": self.fields
                .iter()
                .map(|x| x.schema())
                .collect::<Vec<Value>>()
        })
        .to_string()
    }

    pub fn schema(&self, topic: &str) -> Value {
        json!({
            "type": "record",
            "name": self.schema_name(topic),
            "fields": self.fields
                .iter()
                .map(|x| x.schema())
                .collect::<Vec<Value>>()
        })
    }

    fn fields(model: &Vec<StreamFieldModel>) -> Result<Vec<Field>> {
        let mut result = Vec::with_capacity(model.len());
        for field in model {
            result.push(Field::new(
                &field.name,
                field.optional,
                &field.data_type,
                field.format.as_ref().map(|x| x.as_str()),
            )?);
        }
        Ok(result)
    }
}

impl<'a, R: Read> Iterator for RecordReadIterator<'a, R> {
    type Item = ReadItem<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lines.next().map(|item| match item.result {
            Ok(parsed) => ReadItem {
                raw: item.raw,
                result: self.parent.read_parsed(parsed),
            },
            Err(err) => ReadItem {
                raw: item.raw,
                result: Err(err),
            },
        })
    }
}
