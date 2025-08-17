use arrow_flight::FlightData;
use arrow_ipc::reader::StreamReader;
use tonic::Status;

use crate::types::ProcessingResult;
use crate::validator::SchemaValidator;

pub struct DataProcessor {
    schema_validator: SchemaValidator,
}

impl DataProcessor {
    pub fn new() -> Self {
        Self {
            schema_validator: SchemaValidator::new_with_default_schema(),
        }
    }

    pub fn process_flight_data(
        &self,
        flight_data_messages: Vec<FlightData>,
    ) -> Result<ProcessingResult, Status> {
        let mut count = 0;
        let mut total_bytes = 0;
        let mut total_batches = 0;
        let mut total_rows = 0;

        for (idx, flight_data) in flight_data_messages.iter().enumerate() {
            count += 1;
            total_bytes += flight_data.data_body.len() + flight_data.data_header.len();

            println!(
                "Processing FlightData message {}: {} header bytes, {} body bytes",
                idx + 1,
                flight_data.data_header.len(),
                flight_data.data_body.len()
            );

            if !flight_data.data_body.is_empty() {
                let batch_result = self.process_arrow_data(&flight_data.data_body);
                match batch_result {
                    Ok((batches, rows)) => {
                        total_batches += batches;
                        total_rows += rows;
                    }
                    Err(e) => {
                        return Err(Status::invalid_argument(format!(
                            "Schema validation failed: {}",
                            e
                        )));
                    }
                }
            }
        }

        Ok(ProcessingResult {
            message_count: count,
            total_bytes,
            total_batches,
            total_rows,
        })
    }

    fn process_arrow_data(&self, data: &[u8]) -> Result<(usize, usize), String> {
        self.analyze_data_format(data);

        println!("Attempting to deserialize...");

        let cursor = std::io::Cursor::new(data);
        match StreamReader::try_new(cursor, None) {
            Ok(reader) => {
                println!("Successfully created Arrow StreamReader");
                println!("Schema: {:?}", reader.schema());

                if !self.schema_validator.validate(reader.schema().as_ref()) {
                    println!("Schema validation failed!");
                    println!("Expected: id(Int32), name(Utf8), amount(Float64)");
                    println!("Received: {:?}", reader.schema());
                    return Err(
                        "Schema mismatch: expected schema does not match received schema"
                            .to_string(),
                    );
                }
                println!("Schema validation passed!");

                let mut batch_count = 0;
                let mut row_count = 0;

                for batch_result in reader {
                    match batch_result {
                        Ok(batch) => {
                            batch_count += 1;
                            row_count += batch.num_rows();

                            println!(
                                "Batch {}: {} rows, {} columns",
                                batch_count,
                                batch.num_rows(),
                                batch.num_columns()
                            );

                            self.log_batch_details(&batch);
                        }
                        Err(e) => {
                            println!("Error reading batch: {}", e);
                            break;
                        }
                    }
                }

                println!("Total batches processed: {}", batch_count);
                Ok((batch_count, row_count))
            }
            Err(e) => {
                println!("Failed to create StreamReader: {}", e);
                Err(e.to_string())
            }
        }
    }

    fn analyze_data_format(&self, data: &[u8]) {
        let is_arrow_file =
            data.len() >= 12 && &data[0..6] == b"ARROW1" && &data[data.len() - 6..] == b"ARROW1";

        let has_flatbuffer_header = data.len() >= 8;
        let message_length = if has_flatbuffer_header {
            u32::from_le_bytes([data[0], data[1], data[2], data[3]])
        } else {
            0
        };

        println!("Data analysis:");
        println!("Size: {} bytes", data.len());
        println!("Is Arrow File format: {}", is_arrow_file);
        println!("Has FlatBuffer header: {}", has_flatbuffer_header);
        if has_flatbuffer_header {
            println!("Message length: {} bytes", message_length);
        }
        println!("First 16 bytes: {:02X?}", &data[0..data.len().min(16)]);
        if data.len() > 16 {
            println!("Last 16 bytes: {:02X?}", &data[data.len() - 16..]);
        }
    }

    fn log_batch_details(&self, batch: &arrow::record_batch::RecordBatch) {
        let schema = batch.schema();
        for (col_idx, column) in batch.columns().iter().enumerate() {
            let field = schema.field(col_idx);
            println!(
                "Column '{}' ({}): {:?}",
                field.name(),
                field.data_type(),
                column
            );
        }
    }
}
