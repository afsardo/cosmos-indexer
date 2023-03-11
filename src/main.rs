use dotenv;
use env_logger;
use log::{debug, error, info, warn};
use mongodb::Database;
use queue::Queue;
use serde_json::json;
use std::sync::Arc;
use tokio::time::sleep;
use tokio::time::Duration;

pub mod database;
pub mod hive;
pub mod lcd;
pub mod queue;

pub struct IndexerContext {
    pub config: IndexerConfig,
    pub mongodb: Database,
    pub queue: Queue,
}

#[derive(Debug, Clone)]
pub struct IndexerConfig {
    pub chain_id: String,
    pub lcd_endpoint: String,
    pub hive_endpoint: String,
    pub mongodb_uri: String,
    pub mongodb_database: String,
    pub queue_driver: String,
    pub queue_uri: String,
    pub queue_topic: String,
    pub start_height: i32,
    pub max_block_lag: i32,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = IndexerConfig {
        chain_id: dotenv::var("CHAIN_ID").unwrap(),
        lcd_endpoint: dotenv::var("LCD_ENDPOINT").unwrap(),
        hive_endpoint: dotenv::var("HIVE_ENDPOINT").unwrap(),
        mongodb_uri: dotenv::var("MONGODB_URI").unwrap(),
        mongodb_database: dotenv::var("MONGODB_DATABASE").unwrap(),
        queue_driver: dotenv::var("QUEUE_DRIVER").unwrap(),
        queue_uri: dotenv::var("QUEUE_URI").unwrap(),
        queue_topic: dotenv::var("QUEUE_TOPIC").unwrap(),
        start_height: dotenv::var("START_HEIGHT").unwrap().parse::<i32>().unwrap(),
        max_block_lag: dotenv::var("MAX_BLOCK_LAG")
            .unwrap()
            .parse::<i32>()
            .unwrap(),
    };
    println!("Config: {:?}", &config);

    debug!("Connecting to queue");
    let queue = queue::Queue::new(&config.queue_driver, &config.queue_uri)
        .await
        .unwrap();
    info!("Connected to queue");

    debug!("Connecting to database");
    let mongodb = database::connect(&config.mongodb_uri, &config.mongodb_database)
        .await
        .unwrap();
    info!("Connected to database");

    let context = Arc::new(IndexerContext {
        config,
        mongodb,
        queue,
    });

    let mut last_current_height = lcd::block::fetch_latest_block_height(context.as_ref())
        .await
        .unwrap();

    let mut last_streamed_height = database::stream_status::fetch_streamed_height(context.as_ref())
        .await
        .unwrap();

    if last_streamed_height < context.config.start_height {
        last_streamed_height = context.config.start_height;
    }

    let mut current_block_lag = last_current_height - last_streamed_height;

    info!(
        "Fetched heights: last_current_height: {}, last_streamed_height: {}, current_block_lag: {}",
        last_current_height, last_streamed_height, current_block_lag
    );

    loop {
        let mut block_range: Vec<i32> = vec![];
        let next_block = last_streamed_height + 1;

        current_block_lag = last_current_height - last_streamed_height;

        if current_block_lag > context.config.max_block_lag {
            warn!("Currently behind maximum lag, use batch fetch mode: last_current_height: {}, last_streamed_height: {}, current_block_lag: {}, max_block_lag: {}", last_current_height, last_streamed_height, current_block_lag, context.config.max_block_lag);

            let end_block = next_block + context.config.max_block_lag;
            for block in next_block..end_block {
                block_range.push(block);
            }
        } else {
            if last_streamed_height - last_current_height > context.config.max_block_lag {
                last_current_height = lcd::block::fetch_latest_block_height(context.as_ref())
                    .await
                    .unwrap();
                current_block_lag = last_current_height - last_streamed_height;
            }

            info!("All caught up, keep stream indexing as normal: last_current_height: {}, last_streamed_height: {}, current_block_lag: {}, max_block_lag: {}", last_current_height, last_streamed_height, current_block_lag, context.config.max_block_lag);
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

            for tx in response.data.unwrap().tx.by_height {
                process_tx(context.as_ref(), tx).await;
            }

            last_streamed_height = block_range.get(index).unwrap().clone();
            database::stream_status::update_streamed_height(context.as_ref(), last_streamed_height)
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

async fn process_tx(context: &IndexerContext, tx: hive::txs::TxInfo) {
    if tx.logs.is_none() {
        return;
    }

    debug!("Found tx: height: {}, hash: {}", tx.height, tx.txhash);

    for log in tx.logs.unwrap() {
        if log.events.is_none() {
            continue;
        }

        for event in log.events.unwrap() {
            let packet = json!({
                "chain_id": context.config.chain_id.clone(),
                "height": tx.height,
                "timestamp": tx.timestamp.clone(),
                "txHash": tx.txhash.clone(),
                "event": event,
            });

            let result = context
                .queue
                .publish(&context.config.queue_topic, packet.to_string())
                .await;

            if result.err().is_some() {
                error!("Failed to publish tx event: {:?}", result.err());
            }
        }
    }
}
