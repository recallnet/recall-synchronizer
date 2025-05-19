pub mod error;
pub mod fake;
pub mod s3;
pub mod storage;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub use fake::FakeStorage;
pub use s3::S3Storage;
pub use storage::Storage;
