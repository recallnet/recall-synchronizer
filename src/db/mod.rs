pub mod database;
pub mod error;
#[cfg(test)]
pub mod fake;
pub mod models;
pub mod postgres;
#[cfg(test)]
mod tests;

pub use database::Database;
#[allow(unused_imports)]
pub use error::DatabaseError;
#[cfg(test)]
pub use fake::FakeDatabase;
#[allow(unused_imports)]
pub use models::ObjectIndex;
pub use postgres::PostgresDatabase;
