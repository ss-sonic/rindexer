use std::sync::Arc;

use chrono::{DateTime, Utc};
use ethers::{
    providers::{Middleware, Provider, Ws},
    types::{BlockNumber::Number, Transaction, U256},
};
use futures::StreamExt;
use tracing::{error, info};

use crate::{
    database::postgres::{client::PostgresClient, sql_type_wrapper::EthereumSqlTypeWrapper},
    manifest::network::Network,
};

pub struct NativeTransferIndexer {
    postgres: Arc<PostgresClient>,
    network_configs: Vec<Network>,
    name: String,
}

impl NativeTransferIndexer {
    pub fn new(postgres: Arc<PostgresClient>, network_configs: Vec<Network>, name: String) -> Self {
        Self { postgres, network_configs, name }
    }
    pub async fn start(&self) {
        info!("Starting native transfer indexer 2");
        for config in &self.network_configs {
            let postgres = self.postgres.clone();
            let network = config.name.clone();
            // if ws is not set, we skip
            if config.ws.is_none() {
                continue;
            }
            // Create provider here
            if let Ok(ws) = Provider::<Ws>::connect(&config.ws.clone().unwrap()).await {
                let ws = Arc::new(ws);
                info!("Connected to network: {}", network);
                let indexer_name = self.name.clone();
                tokio::spawn(async move {
                    Self::process_network_transfers(postgres, network, ws, indexer_name).await;
                });
            } else {
                error!("Failed to connect to network: {}", network);
            }
        }
    }

    async fn process_network_transfers(
        postgres: Arc<PostgresClient>,
        network: String,
        ws_client: Arc<Provider<Ws>>,
        indexer_name: String,
    ) {
        info!("Starting native transfer tracking for network: {}", network);

        if let Ok(mut stream) = ws_client.subscribe_blocks().await {
            while let Some(block) = stream.next().await {
                if let Ok(txns) = ws_client.get_block_with_txs(Number(block.number.unwrap())).await
                {
                    if let Some(block) = txns {
                        let mut transfers = Vec::new();

                        for tx in block.transactions {
                            // info!("tx: {:?}", tx);
                            // let _ = Self::store_transfer(
                            //     &postgres,
                            //     network.clone(),
                            //     tx.clone(),
                            //     block.timestamp,
                            // )
                            // .await;
                            transfers.push((network.clone(), tx, block.timestamp));
                        }
                        if !transfers.is_empty() {
                            let _ = Self::bulk_store_transfers(&postgres, transfers, &indexer_name)
                                .await;
                        }
                    }
                }
            }
        }
    }

    async fn bulk_store_transfers(
        postgres: &Arc<PostgresClient>,
        transfers: Vec<(String, Transaction, U256)>,
        indexer_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        if transfers.is_empty() {
            return Ok(());
        }

        let postgres_bulk_data: Vec<Vec<EthereumSqlTypeWrapper>> = transfers
            .iter()
            .map(|(network, tx, timestamp)| {
                vec![
                    EthereumSqlTypeWrapper::DateTime(
                        DateTime::<Utc>::from_timestamp(timestamp.as_u64() as i64, 0).unwrap(),
                    ),
                    EthereumSqlTypeWrapper::String(tx.input.to_string()),
                    EthereumSqlTypeWrapper::Address(tx.to.unwrap_or_default()),
                    EthereumSqlTypeWrapper::Address(tx.from),
                    EthereumSqlTypeWrapper::H256(tx.hash),
                    EthereumSqlTypeWrapper::String(tx.value.to_string()),
                    EthereumSqlTypeWrapper::String(tx.block_number.unwrap_or_default().to_string()),
                    EthereumSqlTypeWrapper::String(tx.nonce.to_string()),
                    EthereumSqlTypeWrapper::String(tx.gas.to_string()),
                    EthereumSqlTypeWrapper::String(tx.gas_price.unwrap_or_default().to_string()),
                    EthereumSqlTypeWrapper::String(network.to_string()),
                ]
            })
            .collect();

        let column_names = vec![
            "timestamp",
            "input",
            "to",
            "from",
            "tx_hash",
            "value",
            "block_number",
            "nonce",
            "gas",
            "gas_price",
            "network",
        ]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();

        let postgres_bulk_column_types =
            postgres_bulk_data[0].iter().map(|param| param.to_type()).collect::<Vec<_>>();

        let bulk_data_length = postgres_bulk_data.len();
        info!("Storing {} transfers", bulk_data_length);
        // info!("postgres_bulk_data: {:?}", postgres_bulk_data);
        let table_name = format!("{}_native.native_transfers", indexer_name.to_lowercase());
        if bulk_data_length > 100 {
            if let Err(e) = postgres
                .bulk_insert_via_copy(
                    &table_name,
                    &column_names,
                    &postgres_bulk_column_types,
                    &postgres_bulk_data,
                )
                .await
            {
                error!(
                    "{}::{} - Error performing bulk insert: {}",
                    table_name, "native_transfers", e
                );
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
            }
        } else if let Err(e) =
            postgres.bulk_insert(&table_name, &column_names, &postgres_bulk_data).await
        {
            {
                error!(
                    "{}::{} - Error performing bulk insert: {}",
                    table_name, "native_transfers", e
                );
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
            }
        }
        Ok(())
    }

    async fn store_transfer(
        postgres: &Arc<PostgresClient>,
        network: String,
        tx: Transaction,
        timestamp: U256,
    ) -> Result<(), Box<dyn std::error::Error + Send>> {
        // Create single transaction data
        let data = vec![
            EthereumSqlTypeWrapper::DateTime(
                DateTime::<Utc>::from_timestamp(timestamp.as_u64() as i64, 0).unwrap(),
            ),
            EthereumSqlTypeWrapper::String(tx.input.to_string()),
            EthereumSqlTypeWrapper::Address(tx.to.unwrap_or_default()),
            EthereumSqlTypeWrapper::Address(tx.from),
            EthereumSqlTypeWrapper::H256(tx.hash),
            EthereumSqlTypeWrapper::String(tx.value.to_string()),
            EthereumSqlTypeWrapper::String(tx.block_number.unwrap_or_default().to_string()),
            EthereumSqlTypeWrapper::String(tx.nonce.to_string()),
            EthereumSqlTypeWrapper::String(tx.gas.to_string()),
            EthereumSqlTypeWrapper::String(tx.gas_price.unwrap_or_default().to_string()),
            EthereumSqlTypeWrapper::String(network),
        ];

        // Add debug logging
        info!("Inserting transaction: {:?}", tx.hash);

        let column_names = vec![
            "timestamp",
            "input",
            "to",
            "from",
            "tx_hash",
            "value",
            "block_number",
            "nonce",
            "gas",
            "gas_price",
            "network",
        ]
        .into_iter()
        .map(String::from)
        .collect::<Vec<_>>();

        info!("column_names: {:?}", column_names);
        info!("data: {:?}", data);

        // Try insertion with explicit error handling
        match postgres.bulk_insert("native_transfers", &column_names, &vec![data]).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to insert transaction {:?}: {:?}", tx.hash, e);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
            }
        }
    }
}
