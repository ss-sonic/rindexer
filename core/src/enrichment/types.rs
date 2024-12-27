use ethers::types::H256;
use uuid::Uuid;

#[derive(Debug)]
pub struct EnrichmentMessage {
    pub table_name: String,
    pub tx_hash: H256,
    pub batch_id: Uuid,
    pub network: String,
}
