use log::debug;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::helpers;
use crate::IndexerContext;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxSearchResponse {
    pub result: TxSearchResult,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxSearchResult {
    pub txs: Vec<Tx>,
    #[serde(deserialize_with = "helpers::deserialize_string_to_u64")]
    pub total_count: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Tx {
    pub hash: String,
    #[serde(deserialize_with = "helpers::deserialize_string_to_u64")]
    pub height: u64,
    pub index: u64,
    pub tx_result: TxResult,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TxResult {
    pub code: i64,
    pub events: Option<Vec<Event>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    #[serde(rename = "type")]
    pub type_str: Option<String>,
    pub attributes: Option<Vec<Attribute>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Attribute {
    #[serde(deserialize_with = "helpers::deserialize_base64_to_string")]
    pub key: Option<String>,
    #[serde(deserialize_with = "helpers::deserialize_base64_to_string")]
    pub value: Option<String>,
}

pub async fn tx_search(
    context: Arc<IndexerContext>,
    from_block_height: u64,
    to_block_height: u64,
) -> Result<Vec<Tx>, anyhow::Error> {
    let mut txs: Vec<Tx> = Vec::new();

    let mut current_page = 1;
    let tx_search_page_response = tx_search_page(
        context.clone(),
        from_block_height,
        to_block_height,
        current_page,
    )
    .await?;
    let total_count = tx_search_page_response.result.total_count;
    txs.extend(tx_search_page_response.result.txs);

    while total_count > txs.len() as u64 {
        current_page += 1;
        debug!("Fetching page: {}", current_page);
        let tx_search_page_response = tx_search_page(
            context.clone(),
            from_block_height,
            to_block_height,
            current_page,
        )
        .await?;
        txs.extend(tx_search_page_response.result.txs);
    }

    txs.sort_by(|a, b| a.index.cmp(&b.index));
    txs.sort_by(|a, b| a.height.cmp(&b.height));
    txs = txs
        .into_iter()
        .filter(|tx| tx.tx_result.code == 0)
        .collect();

    Ok(txs)
}

pub async fn tx_search_page(
    context: Arc<IndexerContext>,
    from_block_height: u64,
    to_block_height: u64,
    page: u64,
) -> Result<TxSearchResponse, anyhow::Error> {
    let client = reqwest::Client::new();

    let mut query = format!("\"tx.height = {}\"", from_block_height);

    if from_block_height != to_block_height {
        query = format!(
            "\"tx.height >= {} AND tx.height <= {}\"",
            from_block_height, to_block_height
        );
    }

    let response = client
        .get(format!("{}/tx_search", context.indexer_config.rpc_endpoint))
        .query(&[
            ("query", query),
            ("page", page.to_string()),
            ("per_page", 100.to_string()),
        ])
        .send()
        .await?
        .json::<TxSearchResponse>()
        .await?;

    Ok(response)
}
