use std::{
    fmt::Debug,
    io::{BufRead, BufReader, Lines, Read},
    iter::Skip,
};

use dyn_clone::{clone_trait_object, DynClone};
use eyre::eyre;
use eyre::Result;

use super::util::ReadItem;

trait LineReader: Debug + DynClone + Send + Sync {
    fn read_line(&self, line: &[u8]) -> Result<Vec<String>>;
}

clone_trait_object!(LineReader);

#[derive(Clone, Debug)]
pub struct Reader {
    header: bool,
    line_reader: Box<dyn LineReader>,
}

pub struct ReadIterator<'a, R: Read> {
    parent: &'a Reader,
    lines: Skip<Lines<BufReader<R>>>,
}

#[derive(Clone, Debug)]
pub struct LineEscapeReader {
    delimiter: u8,
    escape_char: u8,
}

#[derive(Clone, Debug)]
pub struct LineSimpleReader {
    delimiter: u8,
}

impl Reader {
    pub fn new(header: bool, delimiter: u8, escape: Option<u8>) -> Self {
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

    pub fn read_line(&self, line: &[u8]) -> Result<Vec<String>> {
        self.line_reader.read_line(line)
    }

    pub fn read<R: Read>(&self, read: R) -> ReadIterator<R> {
        ReadIterator {
            parent: &self,
            lines: BufReader::new(read)
                .lines()
                .skip(if self.header { 1 } else { 0 }),
        }
    }
}

impl<'a, R: Read> Iterator for ReadIterator<'a, R> {
    type Item = ReadItem<Vec<String>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.lines.next().map(|item| match item {
            Ok(line) => {
                let result = self.parent.read_line(line.as_bytes());
                ReadItem {
                    raw: Some(line),
                    result,
                }
            }
            Err(err) => ReadItem {
                raw: None,
                result: Err(eyre!("{}", err)),
            },
        })
    }
}

impl LineReader for LineEscapeReader {
    fn read_line(&self, line: &[u8]) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut buffer = Vec::with_capacity(line.len());

        let mut escape_flag: bool = false;
        for c in line.iter() {
            if escape_flag {
                buffer.push(*c);
                escape_flag = false;
            } else if *c == self.escape_char {
                escape_flag = true;
            } else if *c == self.delimiter {
                result.push(String::from_utf8(buffer)?);
                buffer = Vec::with_capacity(line.len());
            } else {
                buffer.push(*c);
            }
        }
        Ok(result)
    }
}

impl LineReader for LineSimpleReader {
    fn read_line(&self, line: &[u8]) -> Result<Vec<String>> {
        let mut result = Vec::new();
        let mut buffer = Vec::with_capacity(line.len());

        for c in line.iter() {
            if *c == self.delimiter {
                result.push(String::from_utf8(buffer)?);
                buffer = Vec::with_capacity(line.len());
            } else {
                buffer.push(*c);
            }
        }
        Ok(result)
    }
}
