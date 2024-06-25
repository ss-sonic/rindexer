mod fetch_logs;
pub use fetch_logs::{ContractEventDependencies, EventDependencies, EventsDependencyTree};
mod progress;

use crate::manifest::yaml::Contract;
pub use progress::IndexingEventProgressStatus;
use serde::{Deserialize, Serialize};

mod log_helpers;
pub mod no_code;
mod reorg;
pub mod start;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Indexer {
    pub name: String,

    pub contracts: Vec<Contract>,
}