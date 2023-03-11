use bytes::Bytes;

pub struct NatsQueue {
    client: async_nats::Client,
}

impl NatsQueue {
    pub async fn new(queue_uri: &str) -> Result<NatsQueue, &str> {
        Ok(NatsQueue {
            client: async_nats::connect(queue_uri).await.unwrap(),
        })
    }

    pub async fn publish(&self, topic: &str, message: String) -> Result<(), &str> {
        let result = self
            .client
            .publish(topic.to_string(), Bytes::from(message))
            .await;

        if result.is_err() {
            return Err("Error publishing message to queue");
        }

        Ok(result.unwrap())
    }
}
