use std::fmt::Debug;
use std::task::Poll;

use dyn_clone::{clone_trait_object, DynClone};
use futures::stream::Skip;
use futures::Stream;
use futures::StreamExt;
use pin_project_lite::pin_project;
use stable_eyre::eyre::Report;
use stable_eyre::eyre::Result;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::BufReader;
use tokio_stream::wrappers::LinesStream;

use super::util::ReadItem;

trait LineReader: Debug + DynClone + Send + Sync {
    fn read_line(&self, line: &str) -> Result<Vec<String>>;
}

clone_trait_object!(LineReader);

#[derive(Clone, Debug)]
pub struct Reader {
    header: bool,
    line_reader: Box<dyn LineReader>,
}

pin_project! {
pub struct ReadIterator<'a, R: AsyncRead> {
    #[pin]
    lines: Skip<LinesStream<BufReader<R>>>,
    parent: &'a Reader,
}
}

#[derive(Clone, Debug)]
pub struct LineEscapeReader {
    delimiter: char,
    escape_char: char,
}

#[derive(Clone, Debug)]
pub struct LineSimpleReader {
    delimiter: char,
}

impl Reader {
    pub fn new(header: bool, delimiter: char, escape: Option<char>) -> Self {
        let line_reader: Box<dyn LineReader> = if let Some(escape_char) = escape {
            Box::new(LineEscapeReader {
                delimiter,
                escape_char,
            })
        } else {
            Box::new(LineSimpleReader { delimiter })
        };

        Self {
            header,
            line_reader,
        }
    }

    pub fn read_line(&self, line: &str) -> Result<Vec<String>> {
        self.line_reader.read_line(line)
    }

    pub fn read<R: AsyncRead>(&self, read: R) -> ReadIterator<R> {
        ReadIterator {
            lines: LinesStream::new(BufReader::new(read).lines()).skip(if self.header {
                1
            } else {
                0
            }),
            parent: &self,
        }
    }
}

impl<'a, R: AsyncRead> Stream for ReadIterator<'a, R> {
    type Item = ReadItem<Vec<String>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.project();

        this.lines.poll_next(cx).map(|opt| {
            opt.map(|res| match res {
                Ok(value) => {
                    let r = this.parent.read_line(&value);
                    ReadItem {
                        raw: Some(value),
                        result: r,
                    }
                }
                Err(err) => ReadItem {
                    raw: None,
                    result: Err(Report::new(err)),
                },
            })
        })
    }
}

impl LineReader for LineEscapeReader {
    fn read_line(&self, line: &str) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut buffer = String::with_capacity(line.len());

        let mut escape_flag: bool = false;
        for c in line.chars() {
            if escape_flag {
                buffer.push(c);
                escape_flag = false;
            } else if c == self.escape_char {
                escape_flag = true;
            } else if c == self.delimiter {
                let mut record = buffer.clone();
                record.shrink_to_fit();
                result.push(record);
                buffer.clear();
            } else {
                buffer.push(c);
            }
        }
        Ok(result)
    }
}

impl LineReader for LineSimpleReader {
    fn read_line(&self, line: &str) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut buffer = String::with_capacity(result.len());

        for c in line.chars() {
            if c == self.delimiter {
                let mut record = buffer.clone();
                record.shrink_to_fit();
                result.push(record);
                buffer.clear();
            } else {
                buffer.push(c);
            }
        }
        Ok(result)
    }
}
