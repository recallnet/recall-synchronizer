pub mod database;
pub mod error;
pub mod fake;
pub mod models;
pub mod postgres;
#[cfg(test)]
mod tests;

pub use database::Database;
#[allow(unused_imports)]
pub use error::DatabaseError;
pub use fake::FakeDatabase;
pub use models::ObjectIndex;
pub use postgres::PostgresDatabase;
