use serde::Deserialize;
use std::sync::Arc;

use crate::helpers;
use crate::IndexerContext;

#[derive(Deserialize)]
pub struct BlockchainResponse {
    pub result: BlockchainResult,
}

#[derive(Deserialize)]
pub struct BlockchainResult {
    #[serde(deserialize_with = "helpers::deserialize_string_to_u64")]
    pub last_height: u64,
}

pub async fn fetch_last_block_height(context: Arc<IndexerContext>) -> Result<u64, anyhow::Error> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/blockchain",
            context.indexer_config.rpc_endpoint
        ))
        .send()
        .await?
        .json::<BlockchainResponse>()
        .await?;

    Ok(response.result.last_height)
}
