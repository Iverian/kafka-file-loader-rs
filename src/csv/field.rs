use avro_rs::types::Value;
use serde_json::json;
use stable_eyre::eyre::Result;

use super::engine::Expression;
use super::field_type::CsvFieldItem;
use super::field_type::FieldType;
use super::field_type_impl::field_type_factory;

#[derive(Clone, Debug)]
pub struct Field {
    name: String,
    data_type: Box<dyn FieldType>,
    optional: bool,
    expr: Option<Expression>,
}

impl Field {
    pub fn new(
        name: String,
        optional: bool,
        data_type: String,
        format: Option<String>,
        expr: Option<Expression>,
    ) -> Result<Self> {
        Ok(Self {
            name,
            data_type: field_type_factory(&data_type, format)?,
            optional,
            expr,
        })
    }

    pub fn optional(&self) -> bool {
        self.optional
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn cast(&self, value: Option<String>) -> Result<CsvFieldItem> {
        match value {
            Some(inner) => match &self.expr {
                Some(expr) => self.data_type.cast(expr.evaluate(inner)?),
                None => self.data_type.cast(inner),
            },
            _ => Ok(Value::Null),
        }
    }

    pub fn schema(&self) -> serde_json::Value {
        let data_type = self.data_type.schema();
        json!({
            "name": self.name,
            "type": if self.optional {
                json!(["null", data_type])
            } else {
                data_type
            }
        })
    }
}
