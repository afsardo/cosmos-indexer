use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

use crate::IndexerContext;

#[derive(Debug, Serialize, Deserialize)]
pub struct StreamStatusDocument {
    _id: mongodb::bson::oid::ObjectId,
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "streamedHeight")]
    streamed_height: i32,
    #[serde(rename = "processedHeight")]
    processed_height: i32,
    #[serde(rename = "updatedAt")]
    updated_at: mongodb::bson::DateTime,
}

pub async fn fetch_stream_status(
    context: &IndexerContext,
) -> mongodb::error::Result<StreamStatusDocument> {
    let result = context
        .mongodb
        .collection::<StreamStatusDocument>("stream_status")
        .find_one(
            doc! {
                "chainId": &context.config.chain_id,
            },
            None,
        )
        .await?
        .unwrap();

    Ok(result)
}

pub async fn fetch_streamed_height(context: &IndexerContext) -> mongodb::error::Result<i32> {
    let stream_status = fetch_stream_status(context).await?;

    Ok(stream_status.streamed_height)
}

pub async fn update_streamed_height(
    context: &IndexerContext,
    streamed_height: i32,
) -> mongodb::error::Result<()> {
    context
        .mongodb
        .collection::<StreamStatusDocument>("stream_status")
        .update_one(
            doc! {
                "chainId": &context.config.chain_id,
            },
            doc! {
                "$set": {
                    "streamedHeight": streamed_height,
                    "updatedAt": mongodb::bson::DateTime::from(std::time::SystemTime::now()),
                }
            },
            None,
        )
        .await?;

    Ok(())
}
