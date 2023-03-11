pub struct AWSSQSQueue {}

impl AWSSQSQueue {
    pub async fn new(queue_uri: &str) -> Result<AWSSQSQueue, &str> {
        Ok(AWSSQSQueue {})
    }

    pub async fn publish(&self, topic: &str, message: String) -> Result<(), &str> {
        Err("Not implemented yet")
    }
}
