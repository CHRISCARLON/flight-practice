use arrow_flight::PutResult;
use std::time::Duration;

#[derive(Debug)]
pub struct ProcessingResult {
    pub message_count: usize,
    pub total_bytes: usize,
    pub total_batches: usize,
    pub total_rows: usize,
}

impl ProcessingResult {
    pub fn to_put_result(&self, processing_time: Duration) -> PutResult {
        let metadata = serde_json::json!({
            "messages_received": self.message_count,
            "total_bytes": self.total_bytes,
            "total_batches": self.total_batches,
            "total_rows": self.total_rows,
            "processing_time_ms": processing_time.as_millis(),
            "status": "success",
        })
        .to_string();

        PutResult {
            app_metadata: metadata.into_bytes().into(),
        }
    }
}