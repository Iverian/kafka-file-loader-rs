use std::iter::Iterator;
use std::task::Poll;

use futures::Stream;
use pin_project_lite::pin_project;
use serde_json::json;
use serde_json::Value;
use stable_eyre::eyre::ensure;
use stable_eyre::eyre::eyre;
use stable_eyre::eyre::Context;
use stable_eyre::eyre::Result;
use tokio::io::AsyncRead;

use super::engine::ExprEngine;
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

pin_project! {
pub struct RecordReadIterator<'a, R: AsyncRead> {
    #[pin]
    lines: ReadIterator<'a, R>,
    parent: &'a RecordReader,
}
}

pub type Record<'a> = Vec<(&'a str, CsvFieldItem)>;

impl RecordReader {
    pub fn new(model: StreamSchemaModel) -> Result<Self> {
        let expr_engine = ExprEngine::new();
        let fields = RecordReader::fields(&expr_engine, model.fields)?;
        Ok(Self {
            fields,
            reader: Reader::new(
                model.format.header,
                model.format.delimiter,
                model.format.escape,
            ),
            null_string: model.format.null_string.clone(),
        })
    }

    pub fn read<R: AsyncRead>(&self, read: R) -> RecordReadIterator<R> {
        RecordReadIterator {
            parent: &self,
            lines: self.reader.read(read),
        }
    }

    pub fn read_parsed(&self, mut line: Vec<String>) -> Result<Record> {
        ensure!(line.len() <= self.fields.len(), "too many records");
        let mut result = Vec::new();
        line.reverse();

        for field in self.fields.iter() {
            let mut is_null = true;
            let value = match line.pop() {
                Some(value) => {
                    is_null = value == self.null_string;
                    Some(value)
                }
                None => None,
            };
            ensure!(
                !is_null || field.optional(),
                format!("required field '{}' is missing", field.name())
            );

            result.push((
                field.name(),
                field
                    .cast(value)
                    .map_err(|err| eyre!("error parsing field '{}': {}", field.name(), err))?,
            ));
        }
        Ok(result)
    }

    pub fn read_owned(&self, line: String) -> ReadItem<Record> {
        match self.reader.read_line(&line) {
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

    pub fn schema_str(&self, name: &str) -> String {
        json!({
            "type": "record",
            "name": name,
            "fields": self.fields
                .iter()
                .map(|x| x.schema())
                .collect::<Vec<Value>>()
        })
        .to_string()
    }

    pub fn schema(&self, name: &str) -> Value {
        json!({
            "type": "record",
            "name": name,
            "fields": self.fields
                .iter()
                .map(|x| x.schema())
                .collect::<Vec<Value>>()
        })
    }

    fn fields(engine: &ExprEngine, model: Vec<StreamFieldModel>) -> Result<Vec<Field>> {
        let mut result = Vec::with_capacity(model.len());
        for field in model {
            let field_name = field.name.clone();
            result.push(
                RecordReader::try_create_field(engine, field)
                    .wrap_err(format!("error creating field {:?} from model", &field_name))?,
            );
        }
        Ok(result)
    }

    fn try_create_field(engine: &ExprEngine, field: StreamFieldModel) -> Result<Field> {
        Ok(Field::new(
            field.name,
            field.optional,
            field.data_type,
            field.format,
            match field.expr {
                Some(expr) => Some(engine.compile(expr)?),
                None => None,
            },
        )?)
    }
}

impl<'a, R: AsyncRead> Stream for RecordReadIterator<'a, R> {
    type Item = ReadItem<Record<'a>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.lines.poll_next(cx).map(|opt| {
            opt.map(|item| match item.result {
                Ok(value) => ReadItem {
                    raw: item.raw,
                    result: this.parent.read_parsed(value),
                },
                Err(err) => ReadItem {
                    raw: item.raw,
                    result: Err(err),
                },
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.lines.size_hint()
    }
}
