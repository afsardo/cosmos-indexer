use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::IndexerContext;

pub static EVENTS_COLLECTION: &str = "events";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventsDocument {
    pub _id: mongodb::bson::oid::ObjectId,
    #[serde(rename = "chainId")]
    pub chain_id: String,
    #[serde(rename = "blockHeight")]
    pub block_height: u64,
    #[serde(rename = "txHash")]
    pub tx_hash: String,
    pub key: String,
    pub logs: Vec<EventLog>,
    #[serde(rename = "fullLogs")]
    pub full_logs: Vec<EventLog>,
    #[serde(rename = "createdAt")]
    pub created_at: mongodb::bson::DateTime,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventLog {
    pub key: String,
    pub value: String,
}

pub async fn save_event(
    context: Arc<IndexerContext>,
    block_height: u64,
    tx_hash: String,
    event_key: String,
    event_logs: Vec<(String, String)>,
    event_full_logs: Vec<(String, String)>,
) -> mongodb::error::Result<()> {
    context
        .database
        .collection::<EventsDocument>(EVENTS_COLLECTION)
        .insert_one(
            EventsDocument {
                _id: mongodb::bson::oid::ObjectId::new(),
                chain_id: context.as_ref().indexer_config.chain_id.to_owned(),
                block_height,
                tx_hash: tx_hash.to_owned(),
                key: event_key.to_owned(),
                logs: event_logs
                    .iter()
                    .map(|(k, v)| EventLog {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect(),
                full_logs: event_full_logs
                    .iter()
                    .map(|(k, v)| EventLog {
                        key: k.to_owned(),
                        value: v.to_owned(),
                    })
                    .collect(),
                created_at: mongodb::bson::DateTime::from(std::time::SystemTime::now()),
            },
            None,
        )
        .await?;

    Ok(())
}
