use serde::{Deserialize, Serialize};

use crate::IndexerContext;

#[derive(Deserialize, Debug, Clone)]
pub struct QueryBatch {
    pub data: Option<QueryResponse>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct QueryResponse {
    pub tx: TxData,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TxData {
    #[serde(rename = "byHeight")]
    pub by_height: Vec<TxInfo>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TxInfo {
    pub txhash: String,
    pub height: i32,
    pub timestamp: String,
    pub tx: Tx,
    pub logs: Option<Vec<Log>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Tx {
    pub body: TxBody,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TxBody {
    pub messages: Vec<TxMsg>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TxMsg {
    #[serde(rename = "@type")]
    pub type_str: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Log {
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
    pub key: Option<String>,
    pub value: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct Query {
    pub query: String,
    pub variables: Option<QueryVars>,
}

#[derive(Serialize, Debug)]
pub struct QueryVars {
    pub height: i32,
}

static QUERY: &str = r#"
    query($height: Float!) {
        tx {
            byHeight(height: $height) {
                txhash
                height
                timestamp
                tx {
                    body {
                        messages
                    }
                }
                logs {
                    events {
                        type
                        attributes {
                            key
                            value
                        }
                    }
                }
            }
        }
    }
"#;

pub async fn txs_by_height(context: &IndexerContext, heights: &Vec<i32>) -> Vec<QueryBatch> {
    let client = reqwest::Client::new();

    let mut queries: Vec<Query> = vec![];
    for height in heights.clone() {
        queries.push(Query {
            query: QUERY.to_string(),
            variables: Some(QueryVars { height }),
        });
    }

    let response = client
        .post(&context.indexer_config.hive_endpoint)
        .json(&queries)
        .send()
        .await
        .unwrap()
        .json::<Vec<QueryBatch>>()
        .await;

    if response.is_err() {
        panic!("Error parsing response from Hive: {:?}", response.err());
    }

    return response.unwrap();
}
