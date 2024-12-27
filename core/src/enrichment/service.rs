use std::sync::Arc;

use tokio::sync::mpsc;

use super::types::EnrichmentMessage;
use crate::{database::postgres::client::PostgresClient, provider::JsonRpcCachedProvider};

pub struct InputEnrichmentService {
    sender: mpsc::Sender<EnrichmentMessage>,
}

impl InputEnrichmentService {
    pub fn new(provider: Arc<JsonRpcCachedProvider>, postgres: Arc<PostgresClient>) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        tokio::spawn(Self::run_consumer(provider, postgres, rx));
        Self { sender: tx }
    }

    pub fn get_sender(&self) -> mpsc::Sender<EnrichmentMessage> {
        self.sender.clone()
    }

    async fn run_consumer(
        provider: Arc<JsonRpcCachedProvider>,
        postgres: Arc<PostgresClient>,
        mut rx: mpsc::Receiver<EnrichmentMessage>,
    ) {
        while let Some(msg) = rx.recv().await {
            if let Ok(Some(tx)) = provider.get_transaction(msg.tx_hash).await {
                if let Err(e) = postgres
                    .execute(
                        &format!("UPDATE {} SET input = $1 WHERE tx_hash = $2", msg.table_name),
                        &[&tx.input.to_string(), &msg.tx_hash.to_string()],
                    )
                    .await
                {
                    tracing::error!("Failed to update input for tx {}: {}", msg.tx_hash, e);
                }
            }
        }
    }
}
