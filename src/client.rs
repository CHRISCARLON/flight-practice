use arrow::array::{Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::stream;
use std::sync::Arc;
use tonic::Request;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = FlightServiceClient::connect("http://127.0.0.1:50051").await?;

    println!("Connected to Flight server");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )?;

    println!("Created RecordBatch with {} rows", batch.num_rows());

    use arrow_ipc::writer::StreamWriter;
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    let flight_data_messages = vec![arrow_flight::FlightData {
        flight_descriptor: None,
        data_header: vec![].into(),
        app_metadata: vec![].into(),
        data_body: buf.into(),
    }];

    let stream = stream::iter(flight_data_messages);

    println!("Sending do_put request with Arrow table...");

    let request = Request::new(stream);
    let response = client.do_put(request).await?;

    println!("Response received!");

    let mut response_stream = response.into_inner();
    while let Some(result) = response_stream.message().await? {
        println!("Received PutResult: {:?}", result);
    }

    println!("Test complete!");

    Ok(())
}
