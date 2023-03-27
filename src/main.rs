use dotenv;
use env_logger;
use event_matcher::matcher_config::MatcherConfig;
use log::{debug, info, warn};
use mongodb::Database;
use serde::Deserialize;
use serde::Serialize;
// use queue::Queue;
// use serde_json::json;
use std::sync::Arc;
use tokio::time::sleep;
use tokio::time::Duration;

pub mod database;
pub mod event_matcher;
pub mod hive;
pub mod lcd;
pub mod queue;

pub struct IndexerContext {
    pub indexer_config: IndexerConfig,
    pub matcher_config: MatcherConfig,
    pub mongodb: Database,
    // pub queue: Queue,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexerConfig {
    pub chain_id: String,
    pub lcd_endpoint: String,
    pub hive_endpoint: String,
    pub mongodb_uri: String,
    pub mongodb_database: String,
    // pub queue_driver: String,
    pub start_height: i32,
    pub max_block_lag: i32,
    pub block_batch_size: i32,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    debug!("Parsing indexer config");
    let indexer_config = IndexerConfig {
        chain_id: dotenv::var("CHAIN_ID").unwrap(),
        lcd_endpoint: dotenv::var("LCD_ENDPOINT").unwrap(),
        hive_endpoint: dotenv::var("HIVE_ENDPOINT").unwrap(),
        mongodb_uri: dotenv::var("MONGODB_URI").unwrap(),
        mongodb_database: dotenv::var("MONGODB_DATABASE").unwrap(),
        // queue_driver: dotenv::var("QUEUE_DRIVER").unwrap(),
        start_height: dotenv::var("START_HEIGHT").unwrap().parse::<i32>().unwrap(),
        max_block_lag: dotenv::var("MAX_BLOCK_LAG")
            .unwrap()
            .parse::<i32>()
            .unwrap(),
        block_batch_size: dotenv::var("BLOCK_BATCH_SIZE")
            .unwrap()
            .parse::<i32>()
            .unwrap(),
    };
    info!("Indexer config: {:?}", &indexer_config);

    debug!("Parsing event matcher config");
    let matcher_config = event_matcher::matcher_config::load_matcher_config();
    info!("Matcher config: {:?}", &matcher_config);

    // debug!("Connecting to queue");
    // let queue = queue::Queue::new(&config.queue_driver).await.unwrap();
    // info!("Connected to queue");

    debug!("Connecting to database");
    let mongodb = database::connect(
        &indexer_config.mongodb_uri,
        &indexer_config.mongodb_database,
    )
    .await
    .unwrap();
    info!("Connected to database");

    let context = Arc::new(IndexerContext {
        indexer_config,
        mongodb,
        matcher_config,
        // queue,
    });
    let context_ref = context.as_ref();

    let last_current_height_result = lcd::block::fetch_latest_block_height(context.clone()).await;
    let mut last_current_height = 0;
    if last_current_height_result.is_ok() {
        last_current_height = last_current_height_result.unwrap();
    }

    let last_indexed_height_result =
        database::stream_status::fetch_indexed_height(context.clone()).await;
    let mut last_indexed_height = 0;
    if last_indexed_height_result.is_ok() {
        last_indexed_height = last_indexed_height_result.unwrap();
    }

    if last_indexed_height < context_ref.indexer_config.start_height {
        last_indexed_height = context_ref.indexer_config.start_height;
    }

    let mut current_block_lag = last_current_height - last_indexed_height;

    info!(
        "Fetched heights: last_current_height: {}, last_indexed_height: {}, current_block_lag: {}",
        last_current_height, last_indexed_height, current_block_lag
    );

    loop {
        let mut block_range: Vec<i32> = vec![];
        let next_block = last_indexed_height + 1;

        current_block_lag = last_current_height - last_indexed_height;

        if current_block_lag > context_ref.indexer_config.max_block_lag {
            warn!("Currently behind maximum lag, use batch fetch mode: last_current_height: {}, last_indexed_height: {}, current_block_lag: {}, max_block_lag: {}", last_current_height, last_indexed_height, current_block_lag, context.indexer_config.max_block_lag);

            let end_block = next_block + context.indexer_config.block_batch_size;
            for block in next_block..end_block {
                if block > last_current_height {
                    break;
                }
                block_range.push(block);
            }
        } else {
            if last_indexed_height - last_current_height > context.indexer_config.max_block_lag {
                last_current_height = lcd::block::fetch_latest_block_height(context.clone())
                    .await
                    .unwrap();
                current_block_lag = last_current_height - last_indexed_height;
            }

            info!("All caught up, keep stream indexing as normal: last_current_height: {}, last_indexed_height: {}, current_block_lag: {}, max_block_lag: {}", last_current_height, last_indexed_height, current_block_lag, context.indexer_config.max_block_lag);
            block_range.push(next_block);
        }

        debug!("Checking for new blocks: block_range: {:?}", block_range);

        let responses = hive::txs::txs_by_height(context.as_ref(), &block_range).await;
        for (index, response) in responses.into_iter().enumerate() {
            if response.data.is_none() {
                debug!(
                    "Block not found at height: {:?}",
                    block_range.get(index).unwrap()
                );
                break;
            }

            let mut tasks = Vec::new();
            for tx in response.data.unwrap().tx.by_height {
                tasks.push(tokio::spawn(process_tx(context.clone(), tx)));
            }
            for task in tasks {
                task.await.unwrap();
            }

            last_indexed_height = block_range.get(index).unwrap().clone();
            database::stream_status::update_indexed_height(context.clone(), last_indexed_height)
                .await
                .unwrap();
        }

        if block_range.len() > 1 {
            sleep(Duration::from_millis(100)).await;
        } else {
            sleep(Duration::from_millis(1000)).await;
        }
    }
}

async fn process_tx(context: Arc<IndexerContext>, tx: hive::txs::TxInfo) {
    if tx.logs.is_none() {
        return;
    }

    debug!("Found tx: height: {}, hash: {}", tx.height, tx.txhash);

    let logs = tx.logs.unwrap();
    for log in logs.iter() {
        if log.events.is_none() {
            continue;
        }

        let events = log.events.as_ref().unwrap();
        for event in events.iter() {
            if event.attributes.is_none() || event.type_str.is_none() {
                continue;
            }

            let event_type = event.type_str.as_ref().unwrap();
            if event_type != "wasm" {
                continue;
            }

            let mut all_attributes = Vec::new();
            let mut grouped_attributes = Vec::new();
            let mut current_group = Vec::new();
            let event_attributes = event.attributes.as_ref().unwrap();
            for attribute in event_attributes.iter() {
                if (attribute.key.is_none()) || (attribute.value.is_none()) {
                    continue;
                }

                let key = attribute.key.as_ref().unwrap();
                let value = attribute.value.as_ref().unwrap();

                if key == "_contract_address" {
                    grouped_attributes.push(current_group);
                    current_group = Vec::new();
                }

                current_group.push((key.to_owned(), value.to_owned()));
                all_attributes.push((key.to_owned(), value.to_owned()));
            }

            if current_group.len() > 0 {
                grouped_attributes.push(current_group);
            }

            let mut tasks = Vec::new();
            for log in grouped_attributes {
                tasks.push(tokio::spawn(process_event_matcher(
                    context.clone(),
                    tx.height,
                    tx.txhash.to_owned(),
                    log,
                    all_attributes.clone(),
                )));
            }
            for task in tasks {
                task.await.unwrap();
            }
        }
    }
}

async fn process_event_matcher(
    context: Arc<IndexerContext>,
    tx_height: i32,
    tx_hash: String,
    grouped_logs: Vec<(String, String)>,
    full_logs: Vec<(String, String)>,
) {
    for event in context.as_ref().matcher_config.events.iter() {
        let patterns_found = grouped_logs
            .iter()
            .filter(|(key, value)| {
                return event.patterns.iter().any(|pattern| {
                    return pattern.key == *key && pattern.value == *value;
                });
            })
            .count();

        if event.patterns.len() == patterns_found {
            info!(
                "Found event: {} at height: {} with txHash: {} and logs: {:?}",
                event.name, tx_height, tx_hash, grouped_logs
            );
            database::events::save_event(
                context.clone(),
                tx_height,
                tx_hash.to_owned(),
                event.key.to_owned(),
                grouped_logs.clone(),
                full_logs.clone(),
            )
            .await
            .unwrap();
        }
    }
}
