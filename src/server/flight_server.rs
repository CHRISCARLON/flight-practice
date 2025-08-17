use arrow_flight::{
    FlightData, FlightDescriptor, FlightInfo, PollInfo, PutResult,
    flight_service_server::FlightService,
};
use futures::stream::{self, Stream};
use std::pin::Pin;
use tonic::{Request, Response, Status, Streaming};

use crate::data_processor::DataProcessor;

pub struct SimpleFlightServer {
    data_processor: DataProcessor,
}

impl SimpleFlightServer {
    pub fn new() -> Self {
        Self {
            data_processor: DataProcessor::new(),
        }
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
        let start_time = std::time::Instant::now();
        let mut flight_data_messages = Vec::new();

        while let Some(flight_data) = stream.message().await? {
            flight_data_messages.push(flight_data);
        }

        let processing_result = self.data_processor.process_flight_data(flight_data_messages)?;
        
        let processing_time = start_time.elapsed();
        println!(
            "Total messages received: {}, Total bytes: {}, Processing time: {:?}",
            processing_result.message_count, processing_result.total_bytes, processing_time
        );

        let result = processing_result.to_put_result(processing_time);
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