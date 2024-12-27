#![allow(
    non_camel_case_types,
    clippy::enum_variant_names,
    clippy::too_many_arguments,
    clippy::upper_case_acronyms,
    clippy::type_complexity,
    dead_code
)]
use std::{
    any::Any,
    error::Error,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};

use ethers::{
    abi::Address,
    providers::{Http, Provider, RetryClient},
    types::{Bytes, H256},
};
use rindexer::{
    async_trait,
    event::{
        callback_registry::{
            EventCallbackRegistry, EventCallbackRegistryInformation, EventCallbackResult,
            EventResult, TxInformation,
        },
        contract_setup::{ContractInformation, NetworkContract},
    },
    generate_random_id,
    manifest::{
        contract::{Contract, ContractDetails},
        yaml::read_manifest,
    },
    provider::JsonRpcCachedProvider,
    AsyncCsvAppender, FutureExt, PostgresClient,
};

use super::super::super::super::typings::{
    database::get_or_init_postgres_client, networks::get_provider_cache_for_network,
};
/// THIS IS A GENERATED FILE. DO NOT MODIFY MANUALLY.
///
/// This file was auto generated by rindexer - https://github.com/joshstevens19/rindexer.
/// Any manual changes to this file will be overwritten.
use super::rocket_pool_eth_abi_gen::rindexer_rocket_pool_eth_gen::{
    self, RindexerRocketPoolETHGen,
};

pub type ApprovalData = rindexer_rocket_pool_eth_gen::ApprovalFilter;

#[derive(Debug, Clone)]
pub struct ApprovalResult {
    pub event_data: ApprovalData,
    pub tx_information: TxInformation,
}

pub type TransferData = rindexer_rocket_pool_eth_gen::TransferFilter;

#[derive(Debug, Clone)]
pub struct TransferResult {
    pub event_data: TransferData,
    pub tx_information: TxInformation,
}

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[async_trait]
trait EventCallback {
    async fn call(&self, events: Vec<EventResult>) -> EventCallbackResult<()>;
}

pub struct EventContext<TExtensions>
where
    TExtensions: Send + Sync,
{
    pub database: Arc<PostgresClient>,
    pub csv: Arc<AsyncCsvAppender>,
    pub extensions: Arc<TExtensions>,
}

// didn't want to use option or none made harder DX
// so a blank struct makes interface nice
pub struct NoExtensions {}
pub fn no_extensions() -> NoExtensions {
    NoExtensions {}
}

pub fn approval_handler<TExtensions, F, Fut>(
    custom_logic: F,
) -> ApprovalEventCallbackType<TExtensions>
where
    ApprovalResult: Clone + 'static,
    F: for<'a> Fn(Vec<ApprovalResult>, Arc<EventContext<TExtensions>>) -> Fut
        + Send
        + Sync
        + 'static
        + Clone,
    Fut: Future<Output = EventCallbackResult<()>> + Send + 'static,
    TExtensions: Send + Sync + 'static,
{
    Arc::new(move |results, context| {
        let custom_logic = custom_logic.clone();
        let results = results.clone();
        let context = Arc::clone(&context);
        async move { (custom_logic)(results, context).await }.boxed()
    })
}

type ApprovalEventCallbackType<TExtensions> = Arc<
    dyn for<'a> Fn(
            &'a Vec<ApprovalResult>,
            Arc<EventContext<TExtensions>>,
        ) -> BoxFuture<'a, EventCallbackResult<()>>
        + Send
        + Sync,
>;

pub struct ApprovalEvent<TExtensions>
where
    TExtensions: Send + Sync + 'static,
{
    callback: ApprovalEventCallbackType<TExtensions>,
    context: Arc<EventContext<TExtensions>>,
}

impl<TExtensions> ApprovalEvent<TExtensions>
where
    TExtensions: Send + Sync + 'static,
{
    pub async fn handler<F, Fut>(closure: F, extensions: TExtensions) -> Self
    where
        ApprovalResult: Clone + 'static,
        F: for<'a> Fn(Vec<ApprovalResult>, Arc<EventContext<TExtensions>>) -> Fut
            + Send
            + Sync
            + 'static
            + Clone,
        Fut: Future<Output = EventCallbackResult<()>> + Send + 'static,
    {
        let csv = AsyncCsvAppender::new("/Users/joshstevens/code/rindexer/rindexer_rust_playground/./generated_csv/RocketPoolETH/rocketpooleth-approval.csv");
        if !Path::new("/Users/joshstevens/code/rindexer/rindexer_rust_playground/./generated_csv/RocketPoolETH/rocketpooleth-approval.csv").exists() {
            csv.append_header(vec!["contract_address".into(), "owner".into(), "spender".into(), "value".into(), "tx_hash".into(), "block_number".into(), "block_hash".into(), "network".into(), "tx_index".into(), "log_index".into(), "input".into()])
                .await
                .expect("Failed to write CSV header");
        }

        Self {
            callback: approval_handler(closure),
            context: Arc::new(EventContext {
                database: get_or_init_postgres_client().await,
                csv: Arc::new(csv),
                extensions: Arc::new(extensions),
            }),
        }
    }
}

#[async_trait]
impl<TExtensions> EventCallback for ApprovalEvent<TExtensions>
where
    TExtensions: Send + Sync,
{
    async fn call(&self, events: Vec<EventResult>) -> EventCallbackResult<()> {
        let events_len = events.len();

        // note some can not downcast because it cant decode
        // this happens on events which failed decoding due to
        // not having the right abi for example
        // transfer events with 2 indexed topics cant decode
        // transfer events with 3 indexed topics
        let result: Vec<ApprovalResult> = events
            .into_iter()
            .filter_map(|item| {
                item.decoded_data.downcast::<ApprovalData>().ok().map(|arc| ApprovalResult {
                    event_data: (*arc).clone(),
                    tx_information: item.tx_information,
                })
            })
            .collect();

        if result.len() == events_len {
            (self.callback)(&result, Arc::clone(&self.context)).await
        } else {
            panic!("ApprovalEvent: Unexpected data type - expected: ApprovalData")
        }
    }
}

pub fn transfer_handler<TExtensions, F, Fut>(
    custom_logic: F,
) -> TransferEventCallbackType<TExtensions>
where
    TransferResult: Clone + 'static,
    F: for<'a> Fn(Vec<TransferResult>, Arc<EventContext<TExtensions>>) -> Fut
        + Send
        + Sync
        + 'static
        + Clone,
    Fut: Future<Output = EventCallbackResult<()>> + Send + 'static,
    TExtensions: Send + Sync + 'static,
{
    Arc::new(move |results, context| {
        let custom_logic = custom_logic.clone();
        let results = results.clone();
        let context = Arc::clone(&context);
        async move { (custom_logic)(results, context).await }.boxed()
    })
}

type TransferEventCallbackType<TExtensions> = Arc<
    dyn for<'a> Fn(
            &'a Vec<TransferResult>,
            Arc<EventContext<TExtensions>>,
        ) -> BoxFuture<'a, EventCallbackResult<()>>
        + Send
        + Sync,
>;

pub struct TransferEvent<TExtensions>
where
    TExtensions: Send + Sync + 'static,
{
    callback: TransferEventCallbackType<TExtensions>,
    context: Arc<EventContext<TExtensions>>,
}

impl<TExtensions> TransferEvent<TExtensions>
where
    TExtensions: Send + Sync + 'static,
{
    pub async fn handler<F, Fut>(closure: F, extensions: TExtensions) -> Self
    where
        TransferResult: Clone + 'static,
        F: for<'a> Fn(Vec<TransferResult>, Arc<EventContext<TExtensions>>) -> Fut
            + Send
            + Sync
            + 'static
            + Clone,
        Fut: Future<Output = EventCallbackResult<()>> + Send + 'static,
    {
        let csv = AsyncCsvAppender::new("/Users/joshstevens/code/rindexer/rindexer_rust_playground/./generated_csv/RocketPoolETH/rocketpooleth-transfer.csv");
        if !Path::new("/Users/joshstevens/code/rindexer/rindexer_rust_playground/./generated_csv/RocketPoolETH/rocketpooleth-transfer.csv").exists() {
            csv.append_header(vec!["contract_address".into(), "from".into(), "to".into(), "value".into(), "tx_hash".into(), "block_number".into(), "block_hash".into(), "network".into(), "tx_index".into(), "log_index".into(), "input".into()])
                .await
                .expect("Failed to write CSV header");
        }

        Self {
            callback: transfer_handler(closure),
            context: Arc::new(EventContext {
                database: get_or_init_postgres_client().await,
                csv: Arc::new(csv),
                extensions: Arc::new(extensions),
            }),
        }
    }
}

#[async_trait]
impl<TExtensions> EventCallback for TransferEvent<TExtensions>
where
    TExtensions: Send + Sync,
{
    async fn call(&self, events: Vec<EventResult>) -> EventCallbackResult<()> {
        let events_len = events.len();

        // note some can not downcast because it cant decode
        // this happens on events which failed decoding due to
        // not having the right abi for example
        // transfer events with 2 indexed topics cant decode
        // transfer events with 3 indexed topics
        let result: Vec<TransferResult> = events
            .into_iter()
            .filter_map(|item| {
                item.decoded_data.downcast::<TransferData>().ok().map(|arc| TransferResult {
                    event_data: (*arc).clone(),
                    tx_information: item.tx_information,
                })
            })
            .collect();

        if result.len() == events_len {
            (self.callback)(&result, Arc::clone(&self.context)).await
        } else {
            panic!("TransferEvent: Unexpected data type - expected: TransferData")
        }
    }
}

pub enum RocketPoolETHEventType<TExtensions>
where
    TExtensions: 'static + Send + Sync,
{
    Approval(ApprovalEvent<TExtensions>),
    Transfer(TransferEvent<TExtensions>),
}

pub fn rocket_pool_eth_contract(
    network: &str,
) -> RindexerRocketPoolETHGen<Arc<Provider<RetryClient<Http>>>> {
    let address: Address = "0xae78…6393".parse().expect("Invalid address");
    RindexerRocketPoolETHGen::new(
        address,
        Arc::new(get_provider_cache_for_network(network).get_inner_provider()),
    )
}

pub fn decoder_contract(
    network: &str,
) -> RindexerRocketPoolETHGen<Arc<Provider<RetryClient<Http>>>> {
    if network == "ethereum" {
        RindexerRocketPoolETHGen::new(
            // do not care about address here its decoding makes it easier to handle ValueOrArray
            Address::zero(),
            Arc::new(get_provider_cache_for_network(network).get_inner_provider()),
        )
    } else {
        panic!("Network not supported");
    }
}

impl<TExtensions> RocketPoolETHEventType<TExtensions>
where
    TExtensions: 'static + Send + Sync,
{
    pub fn topic_id(&self) -> &'static str {
        match self {
            RocketPoolETHEventType::Approval(_) => {
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
            }
            RocketPoolETHEventType::Transfer(_) => {
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            }
        }
    }

    pub fn event_name(&self) -> &'static str {
        match self {
            RocketPoolETHEventType::Approval(_) => "Approval",
            RocketPoolETHEventType::Transfer(_) => "Transfer",
        }
    }

    pub fn contract_name(&self) -> String {
        "RocketPoolETH".to_string()
    }

    fn get_provider(&self, network: &str) -> Arc<JsonRpcCachedProvider> {
        get_provider_cache_for_network(network)
    }

    fn decoder(
        &self,
        network: &str,
    ) -> Arc<dyn Fn(Vec<H256>, Bytes) -> Arc<dyn Any + Send + Sync> + Send + Sync> {
        let decoder_contract = decoder_contract(network);

        match self {
            RocketPoolETHEventType::Approval(_) => {
                Arc::new(move |topics: Vec<H256>, data: Bytes| {
                    match decoder_contract.decode_event::<ApprovalData>("Approval", topics, data) {
                        Ok(filter) => Arc::new(filter) as Arc<dyn Any + Send + Sync>,
                        Err(error) => Arc::new(error) as Arc<dyn Any + Send + Sync>,
                    }
                })
            }

            RocketPoolETHEventType::Transfer(_) => {
                Arc::new(move |topics: Vec<H256>, data: Bytes| {
                    match decoder_contract.decode_event::<TransferData>("Transfer", topics, data) {
                        Ok(filter) => Arc::new(filter) as Arc<dyn Any + Send + Sync>,
                        Err(error) => Arc::new(error) as Arc<dyn Any + Send + Sync>,
                    }
                })
            }
        }
    }

    pub fn register(self, manifest_path: &PathBuf, registry: &mut EventCallbackRegistry) {
        let rindexer_yaml = read_manifest(manifest_path).expect("Failed to read rindexer.yaml");
        let topic_id = self.topic_id();
        let contract_name = self.contract_name();
        let event_name = self.event_name();

        let contract_details = rindexer_yaml
            .contracts
            .iter()
            .find(|c| c.name == contract_name)
            .unwrap_or_else(|| {
                panic!(
                    "Contract {} not found please make sure its defined in the rindexer.yaml",
                    contract_name
                )
            })
            .clone();

        let index_event_in_order = contract_details
            .index_event_in_order
            .as_ref()
            .map_or(false, |vec| vec.contains(&event_name.to_string()));

        let contract = ContractInformation {
            name: contract_details.before_modify_name_if_filter_readonly().into_owned(),
            details: contract_details
                .details
                .iter()
                .map(|c| NetworkContract {
                    id: generate_random_id(10),
                    network: c.network.clone(),
                    cached_provider: self.get_provider(&c.network),
                    decoder: self.decoder(&c.network),
                    indexing_contract_setup: c.indexing_contract_setup(),
                    start_block: c.start_block,
                    end_block: c.end_block,
                    disable_logs_bloom_checks: rindexer_yaml
                        .networks
                        .iter()
                        .find(|n| n.name == c.network)
                        .map_or(false, |n| n.disable_logs_bloom_checks.unwrap_or_default()),
                })
                .collect(),
            abi: contract_details.abi,
            reorg_safe_distance: contract_details.reorg_safe_distance.unwrap_or_default(),
        };

        let callback: Arc<
            dyn Fn(Vec<EventResult>) -> BoxFuture<'static, EventCallbackResult<()>> + Send + Sync,
        > = match self {
            RocketPoolETHEventType::Approval(event) => {
                let event = Arc::new(event);
                Arc::new(move |result| {
                    let event = Arc::clone(&event);
                    async move { event.call(result).await }.boxed()
                })
            }

            RocketPoolETHEventType::Transfer(event) => {
                let event = Arc::new(event);
                Arc::new(move |result| {
                    let event = Arc::clone(&event);
                    async move { event.call(result).await }.boxed()
                })
            }
        };

        registry.register_event(EventCallbackRegistryInformation {
            id: generate_random_id(10),
            indexer_name: "RindexerPlayground".to_string(),
            event_name: event_name.to_string(),
            index_event_in_order,
            topic_id: topic_id.parse::<H256>().unwrap(),
            contract,
            callback,
        });
    }
}
