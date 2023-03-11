use aws_sdk_sqs::Client;

pub struct AWSSQSQueue {
    config: AWSSQSConfig,
    client: Client,
}

struct AWSSQSConfig {
    queue_url: String,
    message_group_id: String,
}

impl AWSSQSQueue {
    pub async fn new() -> Result<AWSSQSQueue, &'static str> {
        dotenv::dotenv().ok();

        let shared_config = aws_config::load_from_env().await;

        Ok(AWSSQSQueue {
            config: AWSSQSConfig {
                queue_url: dotenv::var("AWS_SQS_QUEUE_URL").unwrap(),
                message_group_id: dotenv::var("CHAIN_ID").unwrap(),
            },
            client: aws_sdk_sqs::Client::new(&shared_config),
        })
    }

    pub async fn publish(&self, message: String) -> Result<(), &str> {
        self.client
            .send_message()
            .queue_url(self.config.queue_url.to_owned())
            .message_group_id(self.config.message_group_id.to_owned())
            .message_body(message)
            .send()
            .await
            .unwrap();

        Ok(())
    }
}
