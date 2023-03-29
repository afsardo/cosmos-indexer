use serde::Serialize;
use std::error::Error;
use std::sync::Arc;

use crate::IndexerContext;

#[derive(Serialize, Debug, Clone)]
struct LastIndexedHeightMessage {
    chain_id: String,
    last_indexed_height: u64,
}

pub async fn notify_last_indexed_height(
    context: Arc<IndexerContext>,
    last_indexed_height: u64,
) -> Result<(), Box<dyn Error>> {
    if context.sns.is_none() {
        return Err("Notifications are not enabled".into());
    }

    let message = LastIndexedHeightMessage {
        chain_id: context.indexer_config.chain_id.to_owned(),
        last_indexed_height,
    };

    context
        .sns
        .as_ref()
        .unwrap()
        .publish()
        .topic_arn(context.indexer_config.notifications_topic.to_owned())
        .message(serde_json::to_string(&message)?)
        .send()
        .await?;

    Ok(())
}
