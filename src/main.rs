use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, PollInfo, PutResult,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_ipc::reader::StreamReader;
use futures::stream::{self, Stream};
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

pub struct SimpleFlightServer {}

impl SimpleFlightServer {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for SimpleFlightServer {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl FlightService for SimpleFlightServer {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send>>;
    type DoActionStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::ActionType, Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::SchemaResult>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        println!("Received do_put request!");

        let mut stream = request.into_inner();
        let mut count = 0;
        let mut total_bytes = 0;
        let start_time = std::time::Instant::now();

        let mut flight_data_messages = Vec::new();

        while let Some(flight_data) = stream.message().await? {
            count += 1;
            total_bytes += flight_data.data_body.len() + flight_data.data_header.len();
            println!("Received FlightData message #{}", count);
            flight_data_messages.push(flight_data);
        }

        for (idx, flight_data) in flight_data_messages.iter().enumerate() {
            println!(
                "Processing FlightData message {}: {} header bytes, {} body bytes",
                idx + 1,
                flight_data.data_header.len(),
                flight_data.data_body.len()
            );

            if !flight_data.data_body.is_empty() {
                let data = &flight_data.data_body;
                
                // Check for Arrow File format (ARROW1 magic bytes)
                let is_arrow_file = data.len() >= 12 && 
                    &data[0..6] == b"ARROW1" && 
                    &data[data.len()-6..] == b"ARROW1";
                
                // For Arrow Stream: check if it starts with a FlatBuffers message length
                // (4-byte little-endian length, followed by FlatBuffers data)
                let has_flatbuffer_header = data.len() >= 8;
                let message_length = if has_flatbuffer_header {
                    u32::from_le_bytes([data[0], data[1], data[2], data[3]])
                } else {
                    0
                };

                println!("  Data analysis:");
                println!("    Size: {} bytes", data.len());
                println!("    Is Arrow File format: {}", is_arrow_file);
                println!("    Has FlatBuffer header: {}", has_flatbuffer_header);
                if has_flatbuffer_header {
                    println!("    Message length: {} bytes", message_length);
                }
                println!("    First 16 bytes: {:02X?}", &data[0..data.len().min(16)]);
                if data.len() > 16 {
                    println!("    Last 16 bytes: {:02X?}", &data[data.len() - 16..]);
                }

                println!("  Attempting to deserialize...");

                let cursor = std::io::Cursor::new(&flight_data.data_body);
                match StreamReader::try_new(cursor, None) {
                    Ok(reader) => {
                        println!("  Successfully created Arrow StreamReader");
                        println!("  Schema: {:?}", reader.schema());

                        let mut batch_count = 0;
                        for batch_result in reader {
                            match batch_result {
                                Ok(batch) => {
                                    batch_count += 1;
                                    println!(
                                        "  Batch {}: {} rows, {} columns",
                                        batch_count,
                                        batch.num_rows(),
                                        batch.num_columns()
                                    );

                                    let schema = batch.schema();
                                    for (col_idx, column) in batch.columns().iter().enumerate() {
                                        let field = schema.field(col_idx);
                                        println!(
                                            "    Column '{}' ({}): {:?}",
                                            field.name(),
                                            field.data_type(),
                                            column
                                        );
                                    }
                                }
                                Err(e) => {
                                    println!("  Error reading batch: {}", e);
                                    break;
                                }
                            }
                        }
                        println!("  Total batches processed: {}", batch_count);
                    }
                    Err(e) => {
                        println!("  Failed to create StreamReader: {}", e);
                    }
                }
            }
        }

        let processing_time = start_time.elapsed();
        println!(
            "Total messages received: {}, Total bytes: {}, Processing time: {:?}",
            count, total_bytes, processing_time
        );

        let metadata = serde_json::json!({
            "messages_received": count,
            "total_bytes": total_bytes,
            "processing_time_ms": processing_time.as_millis(),
            "status": "success",
        })
        .to_string();

        let result = PutResult {
            app_metadata: metadata.into_bytes().into(),
        };

        let output = stream::once(async { Ok(result) });
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not implemented"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:50051".parse()?;
    let server = SimpleFlightServer::new();

    println!("Starting Arrow Flight server on {}", addr);

    tonic::transport::Server::builder()
        .add_service(FlightServiceServer::new(server))
        .serve(addr)
        .await?;

    Ok(())
}
