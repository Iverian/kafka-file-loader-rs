use chrono::naive::NaiveDate;
use chrono::DateTime;
use erased_serde::Serialize;
use eyre::bail;
use eyre::eyre;
use eyre::Result;
use serde_json::json;
use serde_json::Value;

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
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        match value.to_lowercase().as_str() {
            "t" | "true" | "1" => Ok(Box::new(true)),
            "f" | "false" | "0" => Ok(Box::new(false)),
            _ => Err(eyre!(format!("invalid boolean: {}", value))),
        }
    }

    fn schema(&self) -> Value {
        json!("boolean")
    }
}

impl FieldType for IntType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(value.parse::<i32>()?))
    }

    fn schema(&self) -> Value {
        json!("int")
    }
}

impl FieldType for LongType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(value.parse::<i64>()?))
    }

    fn schema(&self) -> Value {
        json!("long")
    }
}

impl FieldType for FloatType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(value.parse::<f32>()?))
    }

    fn schema(&self) -> Value {
        json!("float")
    }
}

impl FieldType for DoubleType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(value.parse::<f64>()?))
    }

    fn schema(&self) -> Value {
        json!("double")
    }
}

impl DateType {
    fn new(format: Option<&str>) -> Self {
        DateType {
            format: format.map(|x| x.to_owned()),
        }
    }
}

impl FieldType for DateType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(NaiveDate::parse_from_str(
            value,
            match self.format.as_ref() {
                Some(fmt) => fmt,
                None => DATE_FORMAT,
            },
        )?))
    }

    fn schema(&self) -> Value {
        json!("string")
    }
}

impl DateTimeType {
    fn new(format: Option<&str>) -> Self {
        DateTimeType {
            format: format.map(|x| x.to_owned()),
        }
    }
}

impl FieldType for DateTimeType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(match self.format.as_ref() {
            Some(fmt) => DateTime::parse_from_str(value, fmt)?,
            None => DateTime::parse_from_rfc3339(value)?,
        }))
    }

    fn schema(&self) -> Value {
        json!("string")
    }
}

impl FieldType for StringType {
    fn cast(&self, value: &str) -> Result<Box<dyn Serialize>> {
        Ok(Box::new(value.to_owned()))
    }

    fn schema(&self) -> Value {
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

pub fn field_type_factory(data_type: &str, format: Option<&str>) -> Result<Box<dyn FieldType>> {
    match data_type.to_lowercase().as_str() {
        "bool" => Ok(Box::new(BooleanType {})),
        "int" => Ok(Box::new(IntType {})),
        "long" => Ok(Box::new(LongType {})),
        "float" => Ok(Box::new(FloatType {})),
        "double" => Ok(Box::new(DoubleType {})),
        "date" => Ok(Box::new(DateType::new(format))),
        "datetime" => Ok(Box::new(DateTimeType::new(format))),
        "string" => Ok(Box::new(StringType {})),
        _ => Err(eyre!("unknown data type: {}", data_type)),
    }
}
