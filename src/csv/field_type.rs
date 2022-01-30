use std::fmt::Debug;

use dyn_clone::{clone_trait_object, DynClone};
use erased_serde::Serialize;
use eyre::Result;
use serde_json::Value;
pub type CsvFieldItem = Box<dyn Serialize>;

pub trait FieldType: Send + Sync + DynClone + Debug {
    fn cast(&self, value: &str) -> Result<CsvFieldItem>;
    fn schema(&self) -> Value;
}

clone_trait_object!(FieldType);
