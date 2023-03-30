use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::IndexerContext;

static STATUS_COLLECTION: &str = "status";

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusDocument {
    _id: mongodb::bson::oid::ObjectId,
    #[serde(rename = "chainId")]
    chain_id: String,
    #[serde(rename = "indexedHeight")]
    indexed_height: u64,
    #[serde(rename = "updatedAt")]
    updated_at: mongodb::bson::DateTime,
}

pub async fn fetch_indexer_status(
    context: Arc<IndexerContext>,
) -> mongodb::error::Result<StatusDocument> {
    let result = context
        .database
        .collection::<StatusDocument>(STATUS_COLLECTION)
        .find_one(
            doc! {
                "chainId": &context.indexer_config.chain_id,
            },
            None,
        )
        .await?;

    match result {
        Some(status) => Ok(status),
        None => {
            let status = StatusDocument {
                _id: mongodb::bson::oid::ObjectId::new(),
                chain_id: context.indexer_config.chain_id.to_owned(),
                indexed_height: 0,
                updated_at: mongodb::bson::DateTime::from(std::time::SystemTime::now()),
            };

            context
                .database
                .collection::<StatusDocument>(STATUS_COLLECTION)
                .insert_one(&status, None)
                .await?;

            Ok(status)
        }
    }
}

pub async fn fetch_indexed_height(context: Arc<IndexerContext>) -> mongodb::error::Result<u64> {
    let status = fetch_indexer_status(context).await?;

    Ok(status.indexed_height)
}

pub async fn update_indexed_height(
    context: Arc<IndexerContext>,
    indexed_height: u64,
) -> mongodb::error::Result<()> {
    context
        .database
        .collection::<StatusDocument>(STATUS_COLLECTION)
        .update_one(
            doc! {
                "chainId": &context.indexer_config.chain_id,
            },
            doc! {
                "$set": {
                    "indexedHeight": indexed_height as i64,
                    "updatedAt": mongodb::bson::DateTime::from(std::time::SystemTime::now()),
                }
            },
            None,
        )
        .await?;

    Ok(())
}
