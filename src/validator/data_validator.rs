use arrow::datatypes::{DataType, Field, Schema};

pub struct SchemaValidator {
    pub data_schema: Option<Schema>,
}

impl SchemaValidator {
    pub fn new_with_default_schema() -> Self {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, false),
        ]);

        Self {
            data_schema: Some(schema),
        }
    }

    pub fn validate(&self, actual_schema: &Schema) -> bool {
        match &self.data_schema {
            Some(expected) => expected == actual_schema,
            None => true,
        }
    }
}
