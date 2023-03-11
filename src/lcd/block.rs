use serde::{de, Deserialize};

use crate::IndexerContext;

#[derive(Debug, Deserialize)]
pub struct LatestBlockResponse {
    pub block: Block,
}

#[derive(Debug, Deserialize)]
pub struct Block {
    pub header: Header,
}

#[derive(Debug, Deserialize)]
pub struct Header {
    pub version: Version,
    #[serde(deserialize_with = "string_to_i32")]
    pub height: i32,
    pub time: String,
}

#[derive(Debug, Deserialize)]
pub struct Version {
    #[serde(deserialize_with = "string_to_i32")]
    pub block: i32,
    #[serde(deserialize_with = "string_to_i32")]
    pub app: i32,
}

fn string_to_i32<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: &str = de::Deserialize::deserialize(deserializer)?;
    serde_json::from_str(s).map_err(de::Error::custom)
}

pub async fn fetch_latest_block(
    context: &IndexerContext,
) -> Result<LatestBlockResponse, Box<dyn std::error::Error>> {
    let client = reqwest::Client::new();
    let response = client
        .get(format!(
            "{}/cosmos/base/tendermint/v1beta1/blocks/latest",
            context.config.lcd_endpoint
        ))
        .send()
        .await?
        .json::<LatestBlockResponse>()
        .await?;

    Ok(response)
}

pub async fn fetch_latest_block_height(
    context: &IndexerContext,
) -> Result<i32, Box<dyn std::error::Error>> {
    let block = fetch_latest_block(context).await?;

    Ok(block.block.header.height)
}
