use erased_serde::Serialize;
use eyre::Result;
use serde_json::json;
use serde_json::Value;

use super::field_type::FieldType;
use super::field_type_impl::field_type_factory;

#[derive(Clone, Debug)]
pub struct Field {
    name: String,
    optional: bool,
    data_type: Box<dyn FieldType>,
}

impl Field {
    pub fn new(name: &str, optional: bool, data_type: &str, format: Option<&str>) -> Result<Self> {
        Ok(Self {
            name: name.to_owned(),
            optional,
            data_type: field_type_factory(data_type, format)?,
        })
    }

    pub fn optional(&self) -> &bool {
        &self.optional
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn cast(&self, value: Option<&str>) -> Result<Box<dyn Serialize>> {
        match value {
            Some(inner) => self.data_type.cast(inner),
            _ => Ok(Box::new(None::<String>)),
        }
    }

    pub fn schema(&self) -> Value {
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
