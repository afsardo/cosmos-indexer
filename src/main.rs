use dotenv;
use env_logger;
use log::debug;

use cosmos_indexer::IndexerConfig;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    debug!("Parsing indexer config");
    let indexer_config = IndexerConfig {
        chain_id: dotenv::var("CHAIN_ID").unwrap(),
        rpc_endpoint: dotenv::var("RPC_ENDPOINT").unwrap(),
        database_driver: dotenv::var("DATABASE_DRIVER").unwrap(),
        database_uri: dotenv::var("DATABASE_URI").unwrap(),
        database_name: dotenv::var("DATABASE_NAME").unwrap(),
        start_height: dotenv::var("START_HEIGHT").unwrap().parse::<u64>().unwrap(),
        block_lag_batch_size: dotenv::var("BLOCK_LAG_BATCH_SIZE")
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        fetch_batch_timeout: dotenv::var("FETCH_BATCH_TIMEOUT")
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        fetch_single_timeout: dotenv::var("FETCH_SINGLE_TIMEOUT")
            .unwrap()
            .parse::<u64>()
            .unwrap(),
        block_notifications_enabled: dotenv::var("BLOCK_NOTIFICATIONS_ENABLED")
            .unwrap()
            .parse()
            .unwrap(),
        aws_sns_topic: dotenv::var("AWS_SNS_TOPIC").unwrap(),
        aws_localstack: dotenv::var("AWS_LOCALSTACK").unwrap().parse().unwrap(),
        aws_localstack_endpoint: dotenv::var("AWS_LOCALSTACK_ENDPOINT").unwrap(),
    };

    cosmos_indexer::run(indexer_config, None).await;
}
