/// Compatibility layer for backward compatibility with existing code
use crate::config::S3Config;
use crate::s3::{S3Storage, Storage, StorageError};
use bytes::Bytes;

/// Deprecated: Use S3Storage with Storage trait instead
#[derive(Clone)]
pub struct S3Connector {
    inner: S3Storage,
}

impl S3Connector {
    pub async fn new(config: &S3Config) -> anyhow::Result<Self> {
        let inner = S3Storage::new(config)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create S3Storage: {}", e))?;
        Ok(Self { inner })
    }

    pub async fn get_object(&self, key: &str) -> anyhow::Result<Bytes> {
        self.inner.get_object(key).await.map_err(|e| match e {
            StorageError::ObjectNotFound(key) => {
                anyhow::anyhow!("Object with key {} not found", key)
            }
            e => anyhow::anyhow!("S3 error: {}", e),
        })
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use crate::s3::FakeStorage;

    /// Deprecated: Use FakeStorage with Storage trait instead
    #[derive(Clone)]
    pub struct FakeS3Connector {
        inner: FakeStorage,
    }

    impl FakeS3Connector {
        pub fn new(_bucket: &str) -> Self {
            Self {
                inner: FakeStorage::new(),
            }
        }

        pub async fn fake_add_object(&self, key: &str, data: Bytes) {
            let _ = self.inner.add_object(key, data).await;
        }

        pub async fn get_object(&self, key: &str) -> anyhow::Result<Bytes> {
            self.inner.get_object(key).await.map_err(|e| match e {
                StorageError::ObjectNotFound(key) => {
                    anyhow::anyhow!("Object with key {} not found", key)
                }
                e => anyhow::anyhow!("S3 error: {}", e),
            })
        }
    }
}
