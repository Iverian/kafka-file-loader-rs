use std::path::Path;

use eyre::Result;
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};

use crate::csv::field_type_impl::validate_data_type;

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamFieldModel {
    pub name: String,
    #[serde(alias = "type")]
    pub data_type: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default = "get_optional")]
    pub optional: bool,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamFormatModel {
    #[serde(default = "get_header")]
    pub header: bool,
    #[serde(default = "get_delimiter")]
    pub delimiter: u8,
    #[serde(default = "get_escape")]
    pub escape: Option<u8>,
    #[serde(default = "get_null_string")]
    pub null_string: String,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamSchemaModel {
    #[serde(default)]
    pub format: StreamFormatModel,
    pub fields: Vec<StreamFieldModel>,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamInputModel {
    pub dir: String,
    pub pattern: String,
    pub schema: StreamSchemaModel,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamOutputModel {
    pub topic: String,
    #[serde(default)]
    pub value_schema_name: Option<String>,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamModel {
    pub input: StreamInputModel,
    pub output: StreamOutputModel,
}

impl Default for StreamFormatModel {
    fn default() -> Self {
        Self {
            header: get_header(),
            delimiter: get_delimiter(),
            escape: get_escape(),
            null_string: get_null_string(),
        }
    }
}

pub fn get_stream_schema() -> Result<String> {
    let schema = schema_for!(StreamModel);
    let result = serde_yaml::to_string(&schema)?;
    Ok(result)
}

pub fn parse_stream_def(path: &Path) -> Result<StreamModel> {
    let result: StreamModel = serde_yaml::from_reader(std::fs::File::open(path)?)?;
    for i in result.input.schema.fields.iter() {
        validate_data_type(&i.data_type)?;
    }
    Ok(result)
}

fn get_header() -> bool {
    false
}

fn get_delimiter() -> u8 {
    b';'
}

fn get_escape() -> Option<u8> {
    Some(b'\\')
}

fn get_null_string() -> String {
    "-".to_string()
}

fn get_optional() -> bool {
    true
}
