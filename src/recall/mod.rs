use anyhow::Result;
use recall_sdk::network::Network;
use tracing::{debug, info};

use crate::config::RecallConfig;

#[cfg(test)]
pub mod test_utils {
    use super::*;

    #[derive(Clone)]
    pub struct FakeRecallConnector {
        prefix: Option<String>,
    }

    impl FakeRecallConnector {
        pub fn new(prefix: Option<String>) -> Self {
            FakeRecallConnector { prefix }
        }

        pub async fn store_object(&self, key: &str, _data: &[u8]) -> Result<String> {
            let full_key = if let Some(prefix) = &self.prefix {
                format!("{}/{}", prefix, key)
            } else {
                key.to_string()
            };

            debug!("[FAKE] Storing object to Recall: {}", full_key);

            // Return a fake CID
            let fake_cid = format!("bafybeifake{}", key.len());

            debug!(
                "[FAKE] Successfully stored fake object {} with CID: {}",
                full_key, fake_cid
            );
            Ok(fake_cid)
        }
    }
}

// Stub implementation until recall dependencies are re-enabled
#[derive(Clone)]
pub struct RecallConnector {
    #[allow(dead_code)]
    endpoint: String,
    prefix: Option<String>,
}

impl RecallConnector {
    #[allow(dead_code)]
    pub async fn new(config: &RecallConfig) -> Result<Self> {
        let _network = Network::Testnet.init();
        info!(
            "Creating stub Recall connector with endpoint {}",
            &config.endpoint
        );

        Ok(Self {
            endpoint: config.endpoint.clone(),
            prefix: config.prefix.clone(),
        })
    }

    pub async fn store_object(&self, key: &str, _data: &[u8]) -> Result<String> {
        let full_key = if let Some(prefix) = &self.prefix {
            format!("{}/{}", prefix, key)
        } else {
            key.to_string()
        };

        debug!("STUB: Storing object to Recall: {}", full_key);

        // Return a fake CID
        let fake_cid = format!("bafkreistub{}", key.len());

        debug!(
            "STUB: Successfully stored object {} with fake CID: {}",
            full_key, fake_cid
        );
        Ok(fake_cid)
    }

    #[allow(dead_code)]
    pub async fn ensure_bucket_exists(&self, _bucket_name: &str) -> Result<()> {
        debug!("STUB: Ensuring bucket exists (not actually doing anything)");
        Ok(())
    }
}
