use self::{awssqs::AWSSQSQueue, nats::NatsQueue};

pub mod awssqs;
pub mod nats;

pub enum Queue {
    AWSSQS(AWSSQSQueue),
    NATS(NatsQueue),
}

impl Queue {
    pub async fn new(driver: &str) -> Result<Queue, &'static str> {
        match driver {
            "awssqs" => Ok(Queue::AWSSQS(AWSSQSQueue::new().await.unwrap())),
            "nats" => Ok(Queue::NATS(NatsQueue::new().await.unwrap())),
            _ => Err("Queue type not supported"),
        }
    }

    pub async fn publish(&self, message: String) -> Result<(), &str> {
        match self {
            Queue::AWSSQS(q) => q.publish(message).await,
            Queue::NATS(q) => q.publish(message).await,
        }
    }
}
