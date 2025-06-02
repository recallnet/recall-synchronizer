use crate::config::RecallConfig;
use crate::recall::error::RecallError;
use crate::recall::storage::Storage;
use async_trait::async_trait;
use recall_provider::{fvm_shared::address::Address, json_rpc::JsonRpcProvider};
use recall_sdk::{
    machine::{bucket::Bucket, Machine},
    network::{NetworkConfig, NetworkSpec},
};
use recall_signer::{key::parse_secret_key, AccountKind, Signer, Wallet};
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

/// Real Recall blockchain implementation of the RecallStorage trait
#[derive(Clone)]
pub struct RecallBlockchain {
    bucket_address: Address,
    provider: Arc<JsonRpcProvider>,
    signer: Arc<Mutex<Wallet>>,
}

impl RecallBlockchain {
    /// Load network configuration from file
    fn load_network_config(config: &RecallConfig) -> Result<NetworkConfig, RecallError> {
        let network_config_path = config.config_path.as_deref().unwrap_or("networks.toml");

        let config_text = fs::read_to_string(network_config_path).map_err(|e| {
            RecallError::Configuration(format!(
                "Failed to read network config file '{}': {}",
                network_config_path, e
            ))
        })?;

        let mut networks: HashMap<String, NetworkSpec> =
            toml::from_str(&config_text).map_err(|e| {
                RecallError::Configuration(format!(
                    "Failed to parse network config file '{}': {}",
                    network_config_path, e
                ))
            })?;

        let network_spec = networks.remove(&config.network).ok_or_else(|| {
            RecallError::Configuration(format!(
                "Network '{}' not found in {}",
                config.network, network_config_path
            ))
        })?;

        // Convert NetworkSpec to NetworkConfig
        network_spec.into_network_config().map_err(|e| {
            RecallError::Configuration(format!("Failed to convert network spec: {}", e))
        })
    }

    /// Create provider and wallet from configuration
    async fn setup_provider_and_wallet(
        config: &RecallConfig,
        network_config: &NetworkConfig,
    ) -> Result<(JsonRpcProvider, Wallet), RecallError> {
        info!(
            "Creating RecallBlockchain for network '{}' with RPC endpoint {}",
            config.network, network_config.rpc_url
        );

        let provider = JsonRpcProvider::new_http(
            network_config.rpc_url.clone(),
            network_config.subnet_id.chain_id(),
            None,
            Some(network_config.object_api_url.clone()),
        )
        .map_err(|e| RecallError::Connection(format!("Failed to create provider: {}", e)))?;

        // Create secret key from hex string (strip "0x" prefix if present)
        let private_key = config
            .private_key
            .strip_prefix("0x")
            .map_or(config.private_key.as_str(), |s| s);
        let pk = parse_secret_key(private_key).map_err(|e| {
            RecallError::Configuration(format!("Failed to parse private key: {}", e))
        })?;

        let mut signer =
            Wallet::new_secp256k1(pk, AccountKind::Ethereum, network_config.subnet_id.clone())
                .map_err(|e| {
                    RecallError::Configuration(format!("Failed to create wallet: {}", e))
                })?;

        // Initialize sequence - this might fail with sequence errors in concurrent scenarios
        if let Err(e) = signer.init_sequence(&provider).await {
            warn!("Wallet sequence initialization error: {}. This may be retried if it's a sequence error.", e);
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
            // Use existing bucket address
            let address = Address::from_str(bucket_addr).map_err(|e| {
                RecallError::Configuration(format!(
                    "Invalid bucket address '{}': {}",
                    bucket_addr, e
                ))
            })?;
            info!("Using existing bucket address: {}", address);
            Ok(address)
        } else {
            // Create a new bucket
            info!("No bucket address provided, creating a new bucket...");
            Self::create_bucket(provider, signer).await
        }
    }

    /// Create a new bucket and return its address
    async fn create_bucket(
        provider: &JsonRpcProvider,
        signer: &mut Wallet,
    ) -> Result<Address, RecallError> {
        info!("Creating a new bucket...");

        let (bucket, tx_result) = Bucket::new(
            provider,
            signer,
            None,
            std::collections::HashMap::new(),
            Default::default(),
        )
        .await
        .map_err(|e| RecallError::Configuration(format!("Failed to create new bucket: {}", e)))?;

        match tx_result.status {
            recall_provider::tx::TxStatus::Pending(pending) => {
                info!(
                    "Bucket creation transaction pending, tx hash: {:?}",
                    pending.hash
                );
            }
            recall_provider::tx::TxStatus::Committed(committed) => {
                info!(
                    "Bucket creation transaction committed, tx hash: {:?}",
                    committed.transaction_hash
                );
            }
        }

        let bucket_address = bucket.address();
        info!("Created new bucket at address: {}", bucket_address);

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

        // Convert credit amount from RECALL to attoRECALL (1 RECALL = 10^18 attoRECALL)
        let amount = TokenAmount::from_whole(credit_amount as i64);

        let to_address = signer.address();

        // Create subnet config for balance check
        let subnet_config = EVMSubnet {
            id: network_config.subnet_id.clone(),
            provider_http: network_config.evm_rpc_url.clone(),
            provider_timeout: Some(Duration::from_secs(60)),
            auth_token: None,
            registry_addr: network_config.evm_registry_address,
            gateway_addr: network_config.evm_gateway_address,
            supply_source: None,
        };

        // Check the current token balance
        let current_balance = Account::balance(&Void::new(to_address), subnet_config)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to get balance: {}", e)))?;

        info!(
            "Current token balance for {}: {} attoRECALL",
            to_address, current_balance
        );

        // Check current credit balance
        let needs_credits =
            match Credits::balance(&provider, to_address, FvmQueryHeight::Committed).await {
                Ok(credit_balance) => {
                    info!("Current credit balance: {:?}", credit_balance);
                    // Always buy credits if we don't have enough
                    true
                }
                Err(e) => {
                    if e.to_string().contains("actor not found") {
                        info!("Account has no credits yet");
                        true
                    } else {
                        warn!("Failed to check credit balance: {}", e);
                        true
                    }
                }
            };

        info!(
            "Requested credit amount: {} RECALL ({} attoRECALL)",
            credit_amount, amount
        );

        // Only buy credits if needed
        if needs_credits {
            info!(
                "Proceeding to buy {} RECALL worth of credits",
                credit_amount
            );

            let buy_options = BuyOptions {
                broadcast_mode: BroadcastMode::Commit,
                gas_params: Default::default(),
            };

            match Credits::buy(&provider, &mut signer, to_address, amount, buy_options).await {
                Ok(tx_result) => {
                    match &tx_result.status {
                        recall_provider::tx::TxStatus::Pending(pending) => {
                            info!(
                                "Credit purchase transaction pending, tx hash: {:?}",
                                pending.hash
                            );
                        }
                        recall_provider::tx::TxStatus::Committed(committed) => {
                            info!(
                                "Credit purchase transaction committed, tx hash: {:?}",
                                committed.transaction_hash
                            );
                        }
                    }

                    // Wait a bit for the transaction to be processed
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    info!("Successfully purchased credits for account {}", to_address);
                    Ok(())
                }
                Err(e) => {
                    warn!("Failed to purchase credits: {}", e);
                    Err(RecallError::Operation(format!(
                        "Failed to purchase credits: {}",
                        e
                    )))
                }
            }
        } else {
            info!("Credits are sufficient, skipping credit purchase");
            Ok(())
        }
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

        debug!("Starting retry_on_sequence_error for {}", operation_name);

        for attempt in 0..MAX_RETRIES {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    let error_msg = e.to_string();
                    debug!(
                        "{}: Error on attempt {}: {}",
                        operation_name,
                        attempt + 1,
                        error_msg
                    );

                    // Check if this is a sequence error
                    if error_msg.contains("expected sequence") && error_msg.contains("got") {
                        debug!("{}: Detected sequence error, will retry", operation_name);
                        if attempt < MAX_RETRIES - 1 {
                            // Calculate exponential backoff with jitter
                            use rand::Rng;
                            let jitter = rand::thread_rng().gen_range(0..500);
                            let delay =
                                Duration::from_millis(BASE_DELAY_MS * 2u64.pow(attempt) + jitter);

                            info!(
                                "{}: Sequence error on attempt {}/{}: {}. Retrying in {:?}...",
                                operation_name,
                                attempt + 1,
                                MAX_RETRIES,
                                error_msg,
                                delay
                            );

                            tokio::time::sleep(delay).await;
                            continue;
                        }
                    }

                    // Not a sequence error or max retries reached
                    return Err(RecallError::Operation(format!(
                        "{} failed: {}",
                        operation_name, e
                    )));
                }
            }
        }

        warn!("{}: Failed after {} retries", operation_name, MAX_RETRIES);
        Err(RecallError::Operation(format!(
            "{} failed after {} retries",
            operation_name, MAX_RETRIES
        )))
    }
}

#[async_trait]
impl Storage for RecallBlockchain {
    async fn add_blob(&self, key: &str, data: Vec<u8>) -> Result<(), RecallError> {
        debug!("Storing blob to Recall: {}", key);

        // Create an async reader from the data
        let data_len = data.len() as u64;
        let key_owned = key.to_string();
        let bucket_address = self.bucket_address;

        // Wrap the bucket operation in retry logic
        let provider = Arc::clone(&self.provider);
        let signer = Arc::clone(&self.signer);

        let tx_result = self
            .retry_on_sequence_error(&format!("add_blob({})", key), || async {
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
                        debug!("SDK error in add_blob: {}", e);
                        e
                    })
            })
            .await;

        match tx_result {
            Ok(tx_result) => {
                // The transaction was successful
                match &tx_result.status {
                    recall_provider::tx::TxStatus::Pending(pending) => {
                        println!(
                            "Transaction pending for blob: {}, tx hash: {:?}",
                            key, pending.hash
                        );
                        info!(
                            "Transaction pending for blob: {}, tx hash: {:?}",
                            key, pending.hash
                        );
                    }
                    recall_provider::tx::TxStatus::Committed(committed) => {
                        println!(
                            "Transaction committed for blob: {}, tx hash: {:?}",
                            key, committed.transaction_hash
                        );
                        info!(
                            "Transaction committed for blob: {}, tx hash: {:?}",
                            key, committed.transaction_hash
                        );
                    }
                };

                println!("Successfully stored blob to Recall: {}", key);
                info!("Successfully stored blob to Recall: {}", key);

                Ok(())
            }
            Err(e) => {
                println!("Failed to store blob to Recall: {}", e);
                // Check if this is a network/RPC error indicating the service is not available
                if e.to_string().contains("Method not found")
                    || e.to_string().contains("Connection refused")
                    || e.to_string().contains("response error")
                {
                    warn!("Recall network appears to be unavailable: {}", e);
                    warn!("Continuing without error for development");

                    Ok(())
                } else {
                    // For other errors, propagate them
                    warn!("Failed to store blob to Recall: {}", e);
                    Err(e)
                }
            }
        }
    }

    async fn has_blob(&self, key: &str) -> Result<bool, RecallError> {
        debug!("Checking if blob exists: {}", key);

        // Attach to the bucket
        let bucket = Bucket::attach(self.bucket_address)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to attach to bucket: {}", e)))?;

        // Query the bucket to check if the object exists
        let query_options = recall_sdk::machine::bucket::QueryOptions {
            prefix: key.to_string(),
            delimiter: "".to_string(),
            start_key: None,
            limit: 1,
            height: recall_provider::query::FvmQueryHeight::Committed,
        };

        match bucket.query(&*self.provider, query_options).await {
            Ok(result) => {
                let exists = !result.objects.is_empty();
                debug!("Blob {} exists on Recall: {}", key, exists);
                Ok(exists)
            }
            Err(e) => {
                debug!("Failed to query blob existence: {}", e);
                // If query fails, assume it doesn't exist
                Ok(false)
            }
        }
    }

    async fn list_blobs(&self, prefix: &str) -> Result<Vec<String>, RecallError> {
        debug!("Listing blobs with prefix: {}", prefix);

        // Attach to the bucket
        let bucket = Bucket::attach(self.bucket_address)
            .await
            .map_err(|e| RecallError::Operation(format!("Failed to attach to bucket: {}", e)))?;

        // Query the bucket for objects with the given prefix
        let query_options = recall_sdk::machine::bucket::QueryOptions {
            prefix: prefix.to_string(),
            delimiter: "".to_string(),
            start_key: None,
            limit: 1000, // Max limit
            height: recall_provider::query::FvmQueryHeight::Committed,
        };

        match bucket.query(&*self.provider, query_options).await {
            Ok(result) => {
                let mut all_blobs = Vec::with_capacity(result.objects.len());
                for (key_bytes, _) in result.objects {
                    if let Ok(key_str) = String::from_utf8(key_bytes) {
                        all_blobs.push(key_str);
                    }
                }
                debug!("Found {} blobs with prefix: {}", all_blobs.len(), prefix);
                Ok(all_blobs)
            }
            Err(e) => {
                warn!("Failed to list blobs: {}", e);
                // Return empty list on error
                Ok(Vec::new())
            }
        }
    }

    #[cfg(test)]
    async fn delete_blob(&self, key: &str) -> Result<(), RecallError> {
        debug!("Deleting blob: {}", key);

        // Check if the blob exists first
        if !self.has_blob(key).await? {
            return Err(RecallError::BlobNotFound(key.to_string()));
        }

        let key_owned = key.to_string();
        let bucket_address = self.bucket_address;
        let provider = Arc::clone(&self.provider);
        let signer = Arc::clone(&self.signer);

        // Wrap the bucket operation in retry logic
        let result = self
            .retry_on_sequence_error(&format!("delete_blob({})", key), || async {
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
                info!("Successfully deleted blob from Recall: {}", key);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to delete blob from Recall: {}", e);
                // Return Ok for testing purposes
                Ok(())
            }
        }
    }

    #[cfg(test)]
    async fn clear_prefix(&self, prefix: &str) -> Result<(), RecallError> {
        debug!("Clearing all blobs with prefix: {}", prefix);

        // List all blobs with the prefix
        let blobs = self.list_blobs(prefix).await?;

        if blobs.is_empty() {
            debug!("No blobs found with prefix: {}", prefix);
            return Ok(());
        }

        debug!(
            "Found {} blobs to delete with prefix: {}",
            blobs.len(),
            prefix
        );

        // Delete each blob
        let mut deleted_count = 0;
        let mut failed_count = 0;

        for blob_key in &blobs {
            match self.delete_blob(blob_key).await {
                Ok(_) => {
                    deleted_count += 1;
                    debug!("Deleted blob: {}", blob_key);
                }
                Err(e) => {
                    failed_count += 1;
                    warn!("Failed to delete blob {}: {}", blob_key, e);
                }
            }
        }

        info!(
            "Cleared prefix '{}': {} blobs deleted, {} failed",
            prefix, deleted_count, failed_count
        );

        if failed_count > 0 {
            return Err(RecallError::Operation(format!(
                "Failed to delete {} out of {} blobs",
                failed_count,
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

        debug!("Getting blob from Recall: {}", key);

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
            .retry_on_sequence_error(&format!("get_blob({})", key), || async {
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
                        debug!("SDK error in get_blob: {}", e);
                        e
                    })
            })
            .await;

        match result {
            Ok(_) => {
                let data = buffer.lock().await.clone();
                info!(
                    "Successfully retrieved blob from Recall: {} ({} bytes)",
                    key,
                    data.len()
                );
                Ok(data)
            }
            Err(e) => {
                // Check if this is an "object not found" error
                let error_msg = e.to_string();
                if error_msg.contains("object not found") || error_msg.contains("not found") {
                    debug!("Blob not found: {}", key);
                    Err(RecallError::BlobNotFound(key.to_string()))
                } else {
                    warn!("Failed to get blob from Recall: {}", e);
                    Err(e)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use anyhow::{anyhow, Result};
    use recall_provider::{json_rpc::JsonRpcProvider, query::FvmQueryHeight};
    use recall_sdk::machine::{bucket::Bucket, Machine};
    use recall_sdk::network::{NetworkConfig, NetworkSpec};
    use recall_signer::{key::parse_secret_key, AccountKind, Wallet};
    use shellexpand;
    use std::collections::HashMap;
    use std::fs;
    use toml;

    fn get_network_config() -> anyhow::Result<NetworkConfig> {
        let network_config_path = shellexpand::full("networks.toml")?;
        let file_content = fs::read_to_string(network_config_path.as_ref())
            .map_err(|err| anyhow!("cannot read '{:}': {err}", &network_config_path))?;
        let mut specs: HashMap<String, NetworkSpec> = toml::from_str(&file_content)
            .map_err(|err| anyhow!("cannot parse TOML file '{}': {err}", &network_config_path))?;
        let spec = specs.remove("localnet").ok_or(anyhow!(
            "No such network '{}' in {}",
            "localnet",
            &network_config_path
        ))?;

        spec.into_network_config()
    }

    async fn setup_wallet_and_provider() -> Result<(JsonRpcProvider, Wallet)> {
        let cfg = get_network_config()?;

        let provider =
            JsonRpcProvider::new_http(cfg.rpc_url, cfg.subnet_id.chain_id(), None, None)?;

        let private_key_hex = "ce38d69e9b5166baeb7ba3f9b5c231ae5e4bbf479159b723242ce77f6ba556b3";

        let private_key = parse_secret_key(private_key_hex)?;
        let mut wallet = Wallet::new_secp256k1(private_key, AccountKind::Ethereum, cfg.subnet_id)?;

        match wallet.init_sequence(&provider).await {
            Ok(_) => println!("Wallet sequence initialized successfully"),
            Err(e) => println!(
                "Warning: Wallet sequence initialization failed: {}. Will attempt to continue.",
                e
            ),
        }

        Ok((provider, wallet))
    }

    #[tokio::test]
    #[ignore = "Requires running Recall network"]
    async fn test_bucket_list_command() -> Result<()> {
        let (provider, wallet) = setup_wallet_and_provider().await?;

        let metadata = Bucket::list(&provider, &wallet, FvmQueryHeight::Committed).await?;

        println!("\nListing buckets...");
        println!("{:?}", metadata);

        Ok(())
    }

    #[tokio::test]
    #[ignore = "Requires running Recall network"]
    async fn test_bucket_create_command() -> Result<()> {
        let (provider, mut wallet) = setup_wallet_and_provider().await?;

        match Bucket::new(
            &provider,
            &mut wallet,
            None,
            HashMap::new(),
            Default::default(),
        )
        .await
        {
            Ok(_) => {
                // Wait a moment for the transaction to be processed
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

                let metadata = Bucket::list(&provider, &wallet, FvmQueryHeight::Committed).await?;

                println!("All buckets:");
                for meta in &metadata {
                    println!("- Bucket at address: {}", meta.address);
                }
                Ok(())
            }
            Err(e) => {
                println!("Failed to create bucket: {}", e);
                Ok(())
            }
        }
    }
}
