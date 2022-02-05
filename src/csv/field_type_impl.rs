use avro_rs::types::Value;
use chrono::naive::NaiveDate;
use chrono::DateTime;
use chrono::SecondsFormat;
use serde_json::json;
use stable_eyre::eyre::bail;
use stable_eyre::eyre::eyre;
use stable_eyre::eyre::Result;

use super::field_type::CsvFieldItem;
use super::field_type::FieldType;

static DATE_FORMAT: &'static str = "%Y-%m-%d";

lazy_static! {
    static ref FIELD_NAMES: Vec<&'static str> =
        vec!["bool", "int", "long", "float", "double", "date", "datetime", "string"];
}

#[derive(Clone, Debug)]
struct BooleanType {}

#[derive(Clone, Debug)]
struct IntType {}

#[derive(Clone, Debug)]
struct LongType {}

#[derive(Clone, Debug)]
struct FloatType {}

#[derive(Clone, Debug)]
struct DoubleType {}

#[derive(Clone, Debug)]
struct DateType {
    format: Option<String>,
}

#[derive(Clone, Debug)]
struct DateTimeType {
    format: Option<String>,
}

#[derive(Clone, Debug)]
struct StringType {}

impl FieldType for BooleanType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        match value.to_lowercase().as_str() {
            "t" | "true" | "1" => Ok(Value::Boolean(true)),
            "f" | "false" | "0" => Ok(Value::Boolean(false)),
            _ => Err(eyre!(format!("invalid boolean: {}", value))),
        }
    }

    fn schema(&self) -> serde_json::Value {
        json!("boolean")
    }
}

impl FieldType for IntType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::Int(value.parse::<i32>()?))
    }

    fn schema(&self) -> serde_json::Value {
        json!("int")
    }
}

impl FieldType for LongType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::Long(value.parse::<i64>()?))
    }

    fn schema(&self) -> serde_json::Value {
        json!("long")
    }
}

impl FieldType for FloatType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::Float(value.parse::<f32>()?))
    }

    fn schema(&self) -> serde_json::Value {
        json!("float")
    }
}

impl FieldType for DoubleType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::Double(value.parse::<f64>()?))
    }

    fn schema(&self) -> serde_json::Value {
        json!("double")
    }
}

impl DateType {
    fn new(format: Option<String>) -> Self {
        DateType { format }
    }
}

impl FieldType for DateType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::String(
            NaiveDate::parse_from_str(
                &value,
                match self.format.as_ref() {
                    Some(fmt) => fmt,
                    None => DATE_FORMAT,
                },
            )?
            .to_string(),
        ))
    }

    fn schema(&self) -> serde_json::Value {
        json!("string")
    }
}

impl DateTimeType {
    fn new(format: Option<String>) -> Self {
        DateTimeType { format }
    }
}

impl FieldType for DateTimeType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::String(
            match self.format.as_ref() {
                Some(fmt) => DateTime::parse_from_str(&value, fmt)?,
                None => DateTime::parse_from_rfc3339(&value)?,
            }
            .to_rfc3339_opts(SecondsFormat::Micros, true),
        ))
    }

    fn schema(&self) -> serde_json::Value {
        json!("string")
    }
}

impl FieldType for StringType {
    fn cast(&self, value: String) -> Result<CsvFieldItem> {
        Ok(Value::String(value))
    }

    fn schema(&self) -> serde_json::Value {
        json!("string")
    }
}

pub fn validate_data_type(data_type: &str) -> Result<()> {
    let data_type_lower = data_type.to_lowercase();
    if FIELD_NAMES
        .iter()
        .find(|item| **item == data_type_lower)
        .is_none()
    {
        bail!("unknown data type: {}", data_type);
    }
    Ok(())
}

pub fn field_type_factory(data_type: &str, format: Option<String>) -> Result<Box<dyn FieldType>> {
    Ok(match data_type.to_lowercase().as_str() {
        "bool" => Box::new(BooleanType {}),
        "int" => Box::new(IntType {}),
        "long" => Box::new(LongType {}),
        "float" => Box::new(FloatType {}),
        "double" => Box::new(DoubleType {}),
        "string" => Box::new(StringType {}),
        "date" => Box::new(DateType::new(format)),
        "datetime" => Box::new(DateTimeType::new(format)),
        _ => bail!("unknown data type: {}", data_type),
    })
}
