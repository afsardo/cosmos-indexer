use log::{debug, error, info, warn};
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use tokio::time::sleep;
use tokio::time::Duration;

pub mod database;
pub mod event_matcher;
pub mod helpers;
pub mod notifications;
pub mod rpc;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexerConfig {
    pub chain_id: String,
    pub rpc_endpoint: String,
    pub database_driver: String,
    pub database_uri: String,
    pub database_name: String,
    pub start_height: u64,
    pub block_lag_batch_size: u64,
    pub fetch_batch_timeout: u64,
    pub fetch_single_timeout: u64,
    pub block_notifications_enabled: bool,
    pub aws_sns_topic: String,
    pub aws_localstack: bool,
    pub aws_localstack_endpoint: String,
}

pub struct MatcherOptions {
    pub matcher_file_path: Option<String>,
    pub matcher_config: Option<event_matcher::matcher_config::MatcherConfig>,
}

pub struct IndexerContext {
    pub indexer_config: IndexerConfig,
    pub matcher_config: event_matcher::matcher_config::MatcherConfig,
    pub database: mongodb::Database,
    pub sns: Option<aws_sdk_sns::Client>,
}

pub async fn run(indexer_config: IndexerConfig, matcher_options: Option<MatcherOptions>) {
    info!("Indexer config: {:?}", &indexer_config);

    let matcher_options = matcher_options.unwrap_or(MatcherOptions {
        matcher_file_path: None,
        matcher_config: None,
    });

    let matcher_config = if let Some(matcher_config) = matcher_options.matcher_config {
        matcher_config
    } else {
        let matcher_file_path = matcher_options
            .matcher_file_path
            .unwrap_or("config.yaml".to_string());

        debug!("Parsing event matcher config");
        event_matcher::matcher_config::load_matcher_config_from_file(&matcher_file_path)
    };
    info!("Matcher config: {:?}", &matcher_config);

    debug!("Connecting to database");
    let database = database::connect(
        &indexer_config.database_driver,
        &indexer_config.database_uri,
        &indexer_config.database_name,
    )
    .await
    .unwrap();
    info!("Connected to database");

    let sns = if indexer_config.block_notifications_enabled {
        debug!("Connecting to aws sns");
        let aws_shared_config = aws_config::load_from_env().await;
        let mut aws_config_builder = aws_sdk_sns::config::Builder::from(&aws_shared_config);
        if indexer_config.aws_localstack {
            aws_config_builder =
                aws_config_builder.endpoint_url(indexer_config.aws_localstack_endpoint.to_owned());
        }
        let client = Some(aws_sdk_sns::Client::from_conf(aws_config_builder.build()));
        info!("Connected to aws sns");
        client
    } else {
        None
    };

    let context = Arc::new(IndexerContext {
        indexer_config,
        database,
        sns,
        matcher_config,
    });
    let context_ref = context.as_ref();

    let last_indexed_height_result =
        database::stream_status::fetch_indexed_height(context.clone()).await;
    let mut last_indexed_height = 0;
    if last_indexed_height_result.is_ok() {
        last_indexed_height = last_indexed_height_result.unwrap();
    }

    if last_indexed_height < context_ref.indexer_config.start_height {
        last_indexed_height = context_ref.indexer_config.start_height;
    }

    loop {
        let last_current_height = rpc::blockchain::fetch_last_block_height(context.clone())
            .await
            .unwrap();

        let from_block_height = last_indexed_height + 1;
        let mut to_block_height = from_block_height;

        if last_current_height > last_indexed_height {
            let block_lag = last_current_height - last_indexed_height;
            if block_lag > 1 {
                to_block_height = last_indexed_height + context.indexer_config.block_lag_batch_size;
                if to_block_height > last_current_height {
                    to_block_height = last_current_height;
                }
                warn!("Currently behind, fetching in batch mode: last_current_height: {}, last_indexed_height: {}, block_lag: {}", last_current_height, last_indexed_height, block_lag);
            } else {
                info!("All caught up, keep stream indexing as normal: last_current_height: {}, last_indexed_height: {}", last_current_height, last_indexed_height);
            }

            let txs = rpc::txs::tx_search(context.clone(), from_block_height, to_block_height)
                .await
                .unwrap();

            let mut tasks = Vec::new();
            for tx in txs {
                tasks.push(tokio::spawn(process_tx(context.clone(), tx)));
            }
            for task in tasks {
                task.await.unwrap();
            }

            last_indexed_height = to_block_height;
            database::stream_status::update_indexed_height(context.clone(), last_indexed_height)
                .await
                .unwrap();

            if context.indexer_config.block_notifications_enabled {
                let result =
                    notifications::notify_last_indexed_height(context.clone(), last_indexed_height)
                        .await;

                if result.is_err() {
                    error!(
                        "Failed to send notification for height: {}",
                        last_indexed_height
                    );
                }
            }
        }

        if to_block_height - from_block_height > 1 {
            sleep(Duration::from_millis(
                context.indexer_config.fetch_batch_timeout,
            ))
            .await;
        } else {
            sleep(Duration::from_millis(
                context.indexer_config.fetch_single_timeout,
            ))
            .await;
        }
    }
}

async fn process_tx(context: Arc<IndexerContext>, tx: rpc::txs::Tx) {
    if tx.tx_result.code != 0 && tx.tx_result.events.is_none() {
        return;
    }

    debug!("Found tx: height: {}, hash: {}", tx.height, tx.hash);

    let events = tx.tx_result.events.unwrap();

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
                tx.hash.to_owned(),
                log,
                all_attributes.clone(),
            )));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }
}

async fn process_event_matcher(
    context: Arc<IndexerContext>,
    tx_height: u64,
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
