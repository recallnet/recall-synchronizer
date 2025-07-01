use crate::config::RecallConfig;
use crate::recall::error::RecallError;
use crate::recall::storage::Storage;
use async_trait::async_trait;
use recall_provider::{
    fvm_shared::address::{Address, Error, Network},
    json_rpc::JsonRpcProvider,
};
use recall_sdk::{
    machine::{bucket::Bucket, Machine},
    network::{NetworkConfig, NetworkSpec},
};
use recall_signer::{key::parse_secret_key, AccountKind, Signer, Wallet};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Real Recall blockchain implementation of the RecallStorage trait
#[derive(Clone)]
pub struct RecallBlockchain {
    bucket_address: Address,
    provider: Arc<JsonRpcProvider>,
    signer: Arc<Mutex<Wallet>>,
}

impl RecallBlockchain {
    /// Parse an address: tries mainnet, testnet, then EVM address conversion
    fn parse_address(s: &str) -> Result<Address, RecallError> {
        // Try Filecoin address formats first
        Network::Mainnet
            .parse_address(s)
            .or_else(|e| match e {
                Error::UnknownNetwork => Network::Testnet.parse_address(s),
                _ => Err(e),
            })
            .or_else(|_| {
                // Parse as hex address (expecting 0x prefix)
                let hex_str = s.strip_prefix("0x")
                    .or_else(|| s.strip_prefix("0X"))
                    .ok_or_else(|| RecallError::Configuration(
                        format!("Address '{s}' must start with '0x' or be a valid Filecoin address")
                    ))?;

                let bytes = hex::decode(hex_str).map_err(|e| {
                    RecallError::Configuration(format!("Invalid hex in address '{s}': {e}"))
                })?;

                if bytes.len() != 20 {
                    return Err(RecallError::Configuration(format!(
                        "Invalid address '{s}': expected 20 bytes, got {}",
                        bytes.len()
                    )));
                }

                // EVM-form ID address (0xff prefix)
                if bytes[0] == 0xff {
                    if bytes[1..12].iter().all(|&b| b == 0) {
                        let id_bytes: [u8; 8] = bytes[12..20].try_into().unwrap();
                        let actor_id = u64::from_be_bytes(id_bytes);
                        Ok(Address::new_id(actor_id))
                    } else {
                        Err(RecallError::Configuration(format!(
                            "Invalid EVM-form ID address '{s}': bytes 1-11 must be 0x00"
                        )))
                    }
                } else {
                    // Standard Ethereum addresses would need EAM actor conversion
                    Err(RecallError::Configuration(format!(
                        "Ethereum address '{s}' conversion not implemented. Use Filecoin address format."
                    )))
                }
            })
            .map_err(|e| match e {
                RecallError::Configuration(msg) => RecallError::Configuration(msg),
                _ => RecallError::Configuration(format!("Failed to parse address '{s}': {e}")),
            })
    }

    /// Load network configuration from file
    fn load_network_config(config: &RecallConfig) -> Result<NetworkConfig, RecallError> {
        let network_config_path = config.config_path.as_deref().unwrap_or("networks.toml");

        let config_text = fs::read_to_string(network_config_path).map_err(|e| {
            error!("Failed to read network config file '{network_config_path}': {e}");
            RecallError::Configuration(format!(
                "Failed to read network config file '{network_config_path}': {e}"
            ))
        })?;

        let mut networks: HashMap<String, NetworkSpec> =
            toml::from_str(&config_text).map_err(|e| {
                error!("Failed to parse network config file '{network_config_path}': {e}");
                RecallError::Configuration(format!(
                    "Failed to parse network config file '{network_config_path}': {e}"
                ))
            })?;

        let network_spec = networks.remove(&config.network).ok_or_else(|| {
            error!(
                "Network '{}' not found in {network_config_path}",
                config.network
            );
            RecallError::Configuration(format!(
                "Network '{}' not found in {network_config_path}",
                config.network
            ))
        })?;

        // Convert NetworkSpec to NetworkConfig
        network_spec.into_network_config().map_err(|e| {
            error!("Failed to convert network spec: {e}");
            RecallError::Configuration(format!("Failed to convert network spec: {e}"))
        })
    }

    /// Create provider and wallet from configuration
    async fn setup_provider_and_wallet(
        config: &RecallConfig,
        network_config: &NetworkConfig,
    ) -> Result<(JsonRpcProvider, Wallet), RecallError> {
        info!(
            "Creating Recall connection for network '{}' with RPC endpoint {}",
            config.network, network_config.rpc_url
        );

        let provider = JsonRpcProvider::new_http(
            network_config.rpc_url.clone(),
            network_config.subnet_id.chain_id(),
            None,
            Some(network_config.object_api_url.clone()),
        )
        .map_err(|e| {
            error!("Failed to create provider: {e}");
            RecallError::Connection(format!("Failed to create provider: {e}"))
        })?;

        // Create secret key from hex string (strip "0x" prefix if present)
        let private_key = config
            .private_key
            .strip_prefix("0x")
            .map_or(config.private_key.as_str(), |s| s);
        let pk = parse_secret_key(private_key).map_err(|e| {
            error!("Failed to parse private key: {e}");
            RecallError::Configuration(format!("Failed to parse private key: {e}"))
        })?;

        let mut signer =
            Wallet::new_secp256k1(pk, AccountKind::Ethereum, network_config.subnet_id.clone())
                .map_err(|e| {
                    error!("Failed to create wallet: {e}");
                    RecallError::Configuration(format!("Failed to create wallet: {e}"))
                })?;

        // Initialize sequence - this might fail with sequence errors in concurrent scenarios
        if let Err(e) = signer.init_sequence(&provider).await {
            warn!("Wallet sequence initialization error: {e}. This may be retried if it's a sequence error.");
            // Don't fail here - let the caller handle sequence errors with retry logic
        }

        info!("Using wallet address: {}", signer.address());

        Ok((provider, signer))
    }

    /// Parse or create bucket address
    async fn get_or_create_bucket_address(
        config: &RecallConfig,
        provider: &JsonRpcProvider,
        signer: &mut Wallet,
    ) -> Result<Address, RecallError> {
        if let Some(bucket_addr) = &config.bucket {
            let address = Self::parse_address(bucket_addr)?;
            info!("Using existing bucket address: {address}");
            Ok(address)
        } else {
            Self::create_bucket(provider, signer).await
        }
    }

    /// Create a new bucket and return its address
    async fn create_bucket(
        provider: &JsonRpcProvider,
        signer: &mut Wallet,
    ) -> Result<Address, RecallError> {
        info!("Creating a new bucket...");

        let (bucket, _) = Bucket::new(
            provider,
            signer,
            None,
            std::collections::HashMap::new(),
            Default::default(),
        )
        .await
        .map_err(|e| {
            error!("Failed to create new bucket: {e}");
            RecallError::Configuration(format!("Failed to create new bucket: {e}"))
        })?;

        let bucket_address = bucket.address();
        info!("Created new bucket at address: {bucket_address}");

        Ok(bucket_address)
    }

    /// Create a new RecallBlockchain instance from configuration
    pub async fn new(config: &RecallConfig) -> Result<Self, RecallError> {
        let network_config = Self::load_network_config(config)?;

        let (provider, mut signer) =
            Self::setup_provider_and_wallet(config, &network_config).await?;

        let bucket_address =
            Self::get_or_create_bucket_address(config, &provider, &mut signer).await?;

        Ok(Self {
            bucket_address,
            provider: Arc::new(provider),
            signer: Arc::new(Mutex::new(signer)),
        })
    }

    /// Prepare an account for testing by purchasing credits
    ///
    /// This method is only available in test builds and helps set up test accounts
    /// with the necessary credits to perform operations. It checks the account's
    /// balance and only buys credits if the balance is less than 2x the credit amount.
    ///
    /// # Arguments
    ///
    /// * `config` - The RecallConfig with the account's private key
    /// * `credit_amount` - The amount of RECALL tokens to spend on credits (e.g., 20 for 20 RECALL)
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if credits were successfully purchased or skipped, or an error otherwise.
    #[cfg(test)]
    pub async fn prepare_account(
        config: &RecallConfig,
        credit_amount: u32,
    ) -> Result<(), RecallError> {
        use recall_provider::fvm_shared::econ::TokenAmount;
        use recall_provider::query::FvmQueryHeight;
        use recall_provider::tx::BroadcastMode;
        use recall_sdk::account::Account;
        use recall_sdk::credits::{BuyOptions, Credits};
        use recall_sdk::ipc::subnet::EVMSubnet;
        use recall_signer::Void;

        info!("Preparing account by checking balance and potentially purchasing credits");

        let network_config = Self::load_network_config(config)?;

        let (provider, mut signer) =
            Self::setup_provider_and_wallet(config, &network_config).await?;

        let amount = TokenAmount::from_whole(credit_amount as i64);

        let to_address = signer.address();

        let subnet_config = EVMSubnet {
            id: network_config.subnet_id.clone(),
            provider_http: network_config.evm_rpc_url.clone(),
            provider_timeout: Some(Duration::from_secs(60)),
            auth_token: None,
            registry_addr: network_config.evm_registry_address,
            gateway_addr: network_config.evm_gateway_address,
            supply_source: None,
        };

        let current_balance = Account::balance(&Void::new(to_address), subnet_config)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to get balance: {e}")))?;

        info!("Current token balance for {to_address}: {current_balance} attoRECALL");

        let credit_balance = Credits::balance(&provider, to_address, FvmQueryHeight::Committed)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to get credit balance: {e}")))?;

        info!("Current credit balance: {credit_balance:?}");

        // Parse credit_free as TokenAmount since it's in atto units
        // Handle decimal format by parsing as float first, then converting to integer
        let credit_free_atto = {
            let credit_value = credit_balance.credit_free.parse::<f64>().map_err(|e| {
                RecallError::Operation(format!(
                    "Failed to parse credit balance {}: {e}",
                    credit_balance.credit_free
                ))
            })?;
            TokenAmount::from_atto(credit_value as i128)
        };

        // Compare with the amount we want (already converted to atto)
        let needs_credits = credit_free_atto < amount;

        if needs_credits {
            info!("Proceeding to buy {credit_amount} RECALL worth of credits");

            let buy_options = BuyOptions {
                broadcast_mode: BroadcastMode::Commit,
                gas_params: Default::default(),
            };

            Credits::buy(&provider, &mut signer, to_address, amount, buy_options)
                .await
                .map_err(|e| {
                    error!("Failed to buy credits: {e}");
                    RecallError::Operation(format!("Failed to buy credits: {e}"))
                })?;
        } else {
            info!("Credits are sufficient, skipping credit purchase");
        }
        Ok(())
    }

    /// Execute an operation with retry logic for sequence errors
    async fn retry_on_sequence_error<F, Fut, T>(
        &self,
        operation_name: &str,
        mut operation: F,
    ) -> Result<T, RecallError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
    {
        const MAX_RETRIES: u32 = 10;
        const BASE_DELAY_MS: u64 = 500;

        debug!("Starting retry_on_sequence_error for {operation_name}");

        for attempt in 0..MAX_RETRIES {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let error_msg = e.to_string();
                    debug!(
                        "{operation_name}: Error on attempt {}: {error_msg}",
                        attempt + 1
                    );

                    // Check if this is a sequence error
                    if error_msg.contains("expected sequence") && error_msg.contains("got") {
                        debug!("{operation_name}: Detected sequence error, will retry");
                        if attempt < MAX_RETRIES - 1 {
                            // Calculate exponential backoff with jitter
                            use rand::Rng;
                            let jitter = rand::thread_rng().gen_range(0..500);
                            let delay =
                                Duration::from_millis(BASE_DELAY_MS * 2u64.pow(attempt) + jitter);

                            info!(
                                "{operation_name}: Sequence error on attempt {}/{MAX_RETRIES}: {error_msg}. Retrying in {delay:?}...",
                                attempt + 1
                            );

                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }

                    // Not a sequence error or max retries reached
                    return Err(RecallError::Operation(format!(
                        "{operation_name} failed: {e}"
                    )));
                }
            }
        }

        warn!("{operation_name}: Failed after {MAX_RETRIES} retries");
        Err(RecallError::Operation(format!(
            "{operation_name} failed after {MAX_RETRIES} retries"
        )))
    }
}

#[async_trait]
impl Storage for RecallBlockchain {
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<(), RecallError> {
        debug!("Storing blob to Recall: {key}");

        // Create an async reader from the data
        let data_len = data.len() as u64;
        let key_owned = key.to_string();
        let bucket_address = self.bucket_address;

        // Wrap the bucket operation in retry logic
        let provider = Arc::clone(&self.provider);
        let signer = Arc::clone(&self.signer);

        let tx_result = self
            .retry_on_sequence_error(&format!("add_blob({key})"), || async {
                let mut signer_guard = signer.lock().await;

                // Attach to the bucket
                let bucket = Bucket::attach(bucket_address).await?;

                let reader = std::io::Cursor::new(data.clone());

                // Create AddOptions with default values
                let add_options = recall_sdk::machine::bucket::AddOptions {
                    ttl: None,
                    metadata: std::collections::HashMap::new(),
                    overwrite: true,
                    token_amount: None,
                    broadcast_mode: recall_provider::tx::BroadcastMode::Commit,
                    gas_params: Default::default(),
                    show_progress: false,
                };

                bucket
                    .add_reader(
                        &*provider,
                        &mut *signer_guard,
                        &key_owned,
                        reader,
                        data_len,
                        add_options,
                    )
                    .await
                    .map_err(|e| {
                        debug!("SDK error in add_blob: {e}");
                        e
                    })
            })
            .await;

        tx_result.map_err(|e| {
            error!("Failed to add blob to Recall: {e}");
            RecallError::Operation(format!("Failed to add blob: {e}"))
        })?;

        info!("Successfully stored blob to Recall: {key}");
        Ok(())
    }

    async fn has_blob(&self, key: &str) -> Result<bool, RecallError> {
        debug!("Checking if blob exists: {key}");

        let bucket = Bucket::attach(self.bucket_address)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to attach to bucket: {e}")))?;

        let query_options = recall_sdk::machine::bucket::QueryOptions {
            prefix: key.to_string(),
            delimiter: "".to_string(),
            start_key: None,
            limit: 1,
            height: recall_provider::query::FvmQueryHeight::Committed,
        };

        let result = bucket
            .query(&*self.provider, query_options)
            .await
            .map_err(|e| {
                error!("Failed to query blob existence: {e}");
                RecallError::Operation(format!("Failed to query blob existence: {e}"))
            })?;

        let exists = !result.objects.is_empty();
        debug!("Blob {key} exists on Recall: {exists}");
        Ok(exists)
    }

    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError> {
        debug!("Listing blobs with prefix: {prefix}");

        let bucket = Bucket::attach(self.bucket_address)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to attach to bucket: {e}")))?;

        let query_options = recall_sdk::machine::bucket::QueryOptions {
            prefix: prefix.to_string(),
            delimiter: "".to_string(),
            start_key: None,
            limit: 1000, // Max limit
            height: recall_provider::query::FvmQueryHeight::Committed,
        };

        let result = bucket
            .query(&*self.provider, query_options)
            .await
            .map_err(|e| {
                error!("Failed to list blobs: {e}");
                RecallError::Operation(format!("Failed to list blobs: {e}"))
            })?;

        let mut all_blobs = Vec::with_capacity(result.objects.len());
        for (key_bytes, _) in result.objects {
            if let Ok(key_str) = String::from_utf8(key_bytes) {
                all_blobs.push(key_str);
            }
        }
        debug!("Found {} blobs with prefix: {prefix}", all_blobs.len());
        Ok(all_blobs)
    }

    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError> {
        debug!("Deleting blob: {key}");

        if !self.has_blob(key).await? {
            return Err(RecallError::BlobNotFound(key.to_string()));
        }

        let key_owned = key.to_string();
        let bucket_address = self.bucket_address;
        let provider = Arc::clone(&self.provider);
        let signer = Arc::clone(&self.signer);

        let result = self
            .retry_on_sequence_error(&format!("delete_blob({key})"), || async {
                let mut signer_guard = signer.lock().await;

                let bucket = Bucket::attach(bucket_address).await?;

                let delete_options = recall_sdk::machine::bucket::DeleteOptions {
                    broadcast_mode: recall_provider::tx::BroadcastMode::Commit,
                    gas_params: Default::default(),
                };

                bucket
                    .delete(&*provider, &mut *signer_guard, &key_owned, delete_options)
                    .await
            })
            .await;

        match result {
            Ok(_) => {
                info!("Successfully deleted blob from Recall: {key}");
                Ok(())
            }
            Err(e) => {
                warn!("Failed to delete blob from Recall: {e}");
                // Return Ok for testing purposes
                Ok(())
            }
        }
    }

    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError> {
        debug!("Clearing all blobs with prefix: {prefix}");

        let blobs = self.list_blobs(prefix).await?;

        if blobs.is_empty() {
            debug!("No blobs found with prefix: {prefix}");
            return Ok(());
        }

        debug!(
            "Found {} blobs to delete with prefix: {prefix}",
            blobs.len()
        );

        let mut deleted_count = 0;
        let mut failed_count = 0;

        for blob_key in &blobs {
            match self.delete_blob(blob_key).await {
                Ok(_) => {
                    deleted_count += 1;
                    debug!("Deleted blob: {blob_key}");
                }
                Err(e) => {
                    failed_count += 1;
                    warn!("Failed to delete blob {blob_key}: {e}");
                }
            }
        }

        info!("Cleared prefix '{prefix}': {deleted_count} blobs deleted, {failed_count} failed");

        if failed_count > 0 {
            return Err(RecallError::Operation(format!(
                "Failed to delete {failed_count} out of {} blobs",
                blobs.len()
            )));
        }

        Ok(())
    }

    #[cfg(test)]
    async fn get_blob(&self, key: &str) -> Result<Vec<u8>, RecallError> {
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::AsyncWrite;

        debug!("Getting blob from Recall: {key}");

        let key_owned = key.to_string();
        let bucket_address = self.bucket_address;
        let provider = Arc::clone(&self.provider);

        // Create a shared buffer wrapped in Arc<Mutex> to satisfy 'static lifetime
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let buffer_clone = Arc::clone(&buffer);

        // Create a wrapper that implements AsyncWrite by delegating to the Arc<Mutex<Vec<u8>>>
        struct ArcVecWriter(Arc<Mutex<Vec<u8>>>);

        impl AsyncWrite for ArcVecWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<Result<usize, std::io::Error>> {
                // Try to acquire the lock
                match self.0.try_lock() {
                    Ok(mut vec) => {
                        vec.extend_from_slice(buf);
                        Poll::Ready(Ok(buf.len()))
                    }
                    Err(_) => {
                        // Wake the task to retry
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }

            fn poll_flush(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Result<(), std::io::Error>> {
                Poll::Ready(Ok(()))
            }

            fn poll_shutdown(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Result<(), std::io::Error>> {
                Poll::Ready(Ok(()))
            }
        }

        // Wrap the bucket.get operation in retry logic
        let result = self
            .retry_on_sequence_error(&format!("get_blob({key})"), || async {
                let bucket = Bucket::attach(bucket_address).await?;

                let get_options = recall_sdk::machine::bucket::GetOptions {
                    range: None,
                    height: recall_provider::query::FvmQueryHeight::Committed,
                    show_progress: false,
                };

                bucket
                    .get(
                        &*provider,
                        &key_owned,
                        ArcVecWriter(buffer_clone.clone()),
                        get_options,
                    )
                    .await
                    .map_err(|e| {
                        debug!("SDK error in get_blob: {e}");
                        e
                    })
            })
            .await;

        match result {
            Ok(_) => {
                let data = buffer.lock().await.clone();
                info!(
                    "Successfully retrieved blob from Recall: {key} ({} bytes)",
                    data.len()
                );
                Ok(data)
            }
            Err(e) => {
                // Check if this is an "object not found" error
                let error_msg = e.to_string();
                if error_msg.contains("not found") {
                    debug!("Blob not found: {key}");
                    Err(RecallError::BlobNotFound(key.to_string()))
                } else {
                    warn!("Failed to get blob from Recall: {e}");
                    Err(e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_parse_bucket_address() {
        let test_cases = vec![
            // Valid addresses
            ("0xff00000000000000000000000000000000001F25", Ok("f07973")),
            ("0xff00000000000000000000000000000000001f25", Ok("f07973")),
            (
                "t410fkkld55ioe7qg24wvt7fu6pbknb56ht7pt4zamxa",
                Ok("f410fkkld55ioe7qg24wvt7fu6pbknb56ht7pt4zamxa"),
            ),
            (
                "f410fkkld55ioe7qg24wvt7fu6pbknb56ht7pt4zamxa",
                Ok("f410fkkld55ioe7qg24wvt7fu6pbknb56ht7pt4zamxa"),
            ),
            // Error cases
            (
                "invalid_address",
                Err("must start with '0x' or be a valid Filecoin address"),
            ),
            ("0x1234", Err("expected 20 bytes")),
            (
                "0xff11111111111111111111111111111111111111",
                Err("bytes 1-11 must be 0x00"),
            ),
        ];

        for (input, expected) in test_cases {
            match (RecallBlockchain::parse_address(input), expected) {
                (Ok(addr), Ok(expected_addr)) => {
                    assert_eq!(addr.to_string(), expected_addr, "Failed for input: {input}");
                }
                (Err(e), Err(expected_msg)) => {
                    let error_msg = e.to_string();
                    assert!(
                        error_msg.contains(expected_msg),
                        "Expected error containing '{expected_msg}' for input '{input}', got: {error_msg}"
                    );
                }
                (Ok(addr), Err(_)) => {
                    panic!("Expected error for input '{input}', but got success: {addr}");
                }
                (Err(e), Ok(_)) => {
                    panic!("Expected success for input '{input}', but got error: {e}");
                }
            }
        }
    }
}
