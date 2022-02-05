use std::path::{Path, PathBuf};

use regex::Regex;
use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};
use stable_eyre::eyre::{bail, eyre, Result};

use crate::csv::field_type_impl::validate_data_type;

lazy_static! {
    static ref STREAM_DEF_PATTERN: Regex = Regex::new(r"^[a-zA-Z0-9._-]+\.ya?ml$").unwrap();
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamFieldModel {
    pub name: String,
    #[serde(alias = "type")]
    pub data_type: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub expr: Option<String>,
    #[serde(default = "get_optional")]
    pub optional: bool,
}

#[derive(Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StreamFormatModel {
    #[serde(default = "get_header")]
    pub header: bool,
    #[serde(default = "get_delimiter")]
    pub delimiter: char,
    #[serde(default = "get_escape")]
    pub escape: Option<char>,
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
    #[serde(default = "get_pattern")]
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

pub fn check_stream_def_path(path: &Path) -> Result<&Path> {
    if !path.metadata()?.file_type().is_file()
        || !STREAM_DEF_PATTERN.is_match(
            &path
                .file_name()
                .ok_or_else(|| eyre!("path {:?} has no filename component", path))?
                .to_str()
                .ok_or_else(|| eyre!("unable to convert filename to unicode"))?,
        )
    {
        bail!("invalid stream path: {:?}", path);
    }
    Ok(path)
}

pub async fn parse_stream_def(path: PathBuf) -> Result<StreamModel> {
    let result = tokio::task::spawn_blocking(move || -> Result<StreamModel> {
        let result: StreamModel =
            serde_yaml::from_reader(std::fs::File::open(check_stream_def_path(&path)?)?)?;
        Ok(result)
    })
    .await??;
    for i in result.input.schema.fields.iter() {
        validate_data_type(&i.data_type)?;
    }
    Ok(result)
}

fn get_header() -> bool {
    false
}

fn get_delimiter() -> char {
    ';'
}

fn get_escape() -> Option<char> {
    Some('\\')
}

fn get_null_string() -> String {
    "-".to_string()
}

fn get_optional() -> bool {
    true
}

fn get_pattern() -> String {
    ".*".to_string()
}
