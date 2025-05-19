pub mod compat;
pub mod error;
pub mod fake;
pub mod real_s3;
pub mod storage;
#[cfg(test)]
mod tests;

pub use error::StorageError;
#[cfg(test)]
pub use fake::FakeStorage;
pub use real_s3::S3Storage;
pub use storage::Storage;

// Backward compatibility
#[cfg(test)]
pub use compat::test_utils;
pub use compat::S3Connector;
