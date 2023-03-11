use bytes::Bytes;

pub struct NatsQueue {
    config: NatsConfig,
    client: async_nats::Client,
}

struct NatsConfig {
    topic: String,
}

impl NatsQueue {
    pub async fn new() -> Result<NatsQueue, &'static str> {
        dotenv::dotenv().ok();

        let nats_uri = dotenv::var("NATS_URI").unwrap();

        let config = NatsConfig {
            topic: dotenv::var("NATS_TOPIC").unwrap(),
        };

        Ok(NatsQueue {
            config,
            client: async_nats::connect(nats_uri).await.unwrap(),
        })
    }

    pub async fn publish(&self, message: String) -> Result<(), &str> {
        let result = self
            .client
            .publish(self.config.topic.to_owned(), Bytes::from(message))
            .await;

        if result.is_err() {
            return Err("Error publishing message to queue");
        }

        Ok(result.unwrap())
    }
}
