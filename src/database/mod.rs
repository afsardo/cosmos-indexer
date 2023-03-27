use mongodb::Client;
use mongodb::Database;

pub mod events;
pub mod stream_status;

pub async fn connect(database_url: &str, database_name: &str) -> mongodb::error::Result<Database> {
    let client = Client::with_uri_str(database_url).await?;
    let database = client.database(database_name);

    Ok(database)
}
