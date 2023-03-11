use self::{awssqs::AWSSQSQueue, nats::NatsQueue};

pub mod awssqs;
pub mod nats;

pub enum Queue {
    AWSSQS(AWSSQSQueue),
    NATS(NatsQueue),
}

impl Queue {
    pub async fn new(driver: &str, queue_uri: &str) -> Result<Queue, &'static str> {
        match driver {
            "awssqs" => Ok(Queue::AWSSQS(AWSSQSQueue::new(queue_uri).await.unwrap())),
            "nats" => Ok(Queue::NATS(NatsQueue::new(queue_uri).await.unwrap())),
            _ => Err("Queue type not supported"),
        }
    }

    pub async fn publish(&self, topic: &str, message: String) -> Result<(), &str> {
        match self {
            Queue::AWSSQS(q) => q.publish(topic, message).await,
            Queue::NATS(q) => q.publish(topic, message).await,
        }
    }
}
