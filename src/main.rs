mod server;
mod data_processor;
mod types;

use arrow_flight::flight_service_server::FlightServiceServer;
use server::SimpleFlightServer;

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