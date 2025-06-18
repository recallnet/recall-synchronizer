pub mod database;
pub mod error;
#[cfg(test)]
pub mod fake;
pub mod models;
pub mod pg_schema;
pub mod postgres;
#[cfg(test)]
mod tests;

pub use database::Database;
#[allow(unused_imports)]
pub use error::DatabaseError;
#[cfg(test)]
pub use fake::FakeDatabase;
pub use models::ObjectIndex;
#[allow(unused_imports)]
pub use postgres::PostgresDatabase;
