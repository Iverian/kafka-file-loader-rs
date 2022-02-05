use std::fmt::Debug;

use dyn_clone::{clone_trait_object, DynClone};
use stable_eyre::eyre::Result;

pub type CsvFieldItem = avro_rs::types::Value;

pub trait FieldType: Send + Sync + DynClone + Debug {
    fn cast(&self, value: String) -> Result<CsvFieldItem>;
    fn schema(&self) -> serde_json::Value;
}

clone_trait_object!(FieldType);
