use crate::db::{Database, DatabaseError, FakeDatabase, PostgresDatabase};
use crate::test_utils::{create_test_object_index, load_test_config};
use chrono::{Duration, Utc};
use std::sync::Arc;

// Type alias to simplify the complex type for database factory functions
type DatabaseFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Box<dyn Database + Send + Sync>>>;

// Static reference to be shared between test runs
// SAFETY: This is safe because:
// 1. It's only accessed during testing
// 2. It's only written to once during initialization
// 3. After initialization, it's only read and never written to again
// 4. Test runners generally ensure proper isolation between concurrent tests
#[allow(static_mut_refs)]
static mut PG_DATABASE: Option<Arc<PostgresDatabase>> = None;

/// Creates or returns a cached PostgreSQL database connection
///
/// This function provides a singleton pattern for the database connection in tests.
/// It ensures that only one connection is established during testing, which is reused
/// across test cases.
async fn get_or_create_postgres_connection() -> Result<Arc<PostgresDatabase>, String> {
    // Check if we need to initialize the connection
    #[allow(static_mut_refs)]
    let pg_database_initialized = unsafe { PG_DATABASE.is_some() };

    if pg_database_initialized {
        println!("- Using cached PostgreSQL connection");
        // Return the cached connection
        // SAFETY: We've already checked that it's initialized and it's only read after initialization
        #[allow(static_mut_refs)]
        return Ok(unsafe { PG_DATABASE.as_ref().unwrap().clone() });
    }

    // Get database URL from test config
    let test_config = load_test_config();
    let db_url = test_config.database.url;

    let pg_db = PostgresDatabase::new(&db_url)
        .await
        .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

    let pg_db = Arc::new(pg_db);

    // Store the database connection for future use
    // SAFETY: This is safe because:
    // 1. We're only writing to this static once
    // 2. After this point, the variable is only read, never written to again
    unsafe {
        PG_DATABASE = Some(pg_db.clone());
    }

    Ok(pg_db)
}

// Helper function to create test databases
fn get_test_databases() -> Vec<DatabaseFactory> {
    let mut databases: Vec<DatabaseFactory> = vec![
        // Always include the FakeDatabase
        Box::new(|| {
            let future = async {
                println!("- Using FakeDatabase implementation");
                Box::new(FakeDatabase::new()) as Box<dyn Database + Send + Sync>
            };

            Box::pin(future)
        }),
    ];

    let test_config = load_test_config();

    // Conditionally add the real PostgreSQL implementation when enabled
    if test_config.database.enabled {
        databases.push(Box::new(|| {
            Box::pin(async {
                match get_or_create_postgres_connection().await {
                    Ok(db) => {
                        println!("- Using PostgresDatabase implementation");
                        Box::new(db) as Box<dyn Database + Send + Sync>
                    }
                    Err(e) => {
                        panic!(
                            "Failed to connect to PostgreSQL: {}. Set database.enabled=false in test_config.toml to skip these tests.",
                            e
                        );
                    }
                }
            })
        }));
    }

    databases
}

#[tokio::test]
async fn test_get_objects_to_sync() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;

        // Add test objects with different timestamps
        let now = Utc::now();
        let object1 = create_test_object_index("test/object1.jsonl", now);

        // Create object2 with modified timestamp
        let mut object2 = create_test_object_index("test/object2.jsonl", now + Duration::hours(1));
        object2.size_bytes = Some(2048); // Customize size if needed

        db.add_object(object1).await.unwrap();
        db.add_object(object2).await.unwrap();

        // Test query with no timestamp restriction
        let objects = db.get_objects_to_sync(10, None).await.unwrap();
        assert!(!objects.is_empty(), "Should return objects");

        // Test query with timestamp restriction (future)
        let future = Utc::now() + Duration::days(1);
        let objects = db.get_objects_to_sync(10, Some(future)).await.unwrap();
        assert_eq!(
            objects.len(),
            0,
            "Should not return objects modified before the timestamp"
        );

        // Test with limit
        let objects = db.get_objects_to_sync(1, None).await.unwrap();
        assert_eq!(
            objects.len(),
            1,
            "Should only return one object with limit=1"
        );
    }
}

#[tokio::test]
async fn test_get_object_by_key() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;

        // Add a test object
        let now = Utc::now();
        let object1 = create_test_object_index("test/object1.jsonl", now);

        db.add_object(object1).await.unwrap();

        // Test retrieving the object
        let object = db.get_object_by_key("test/object1.jsonl").await.unwrap();
        assert_eq!(object.object_key, "test/object1.jsonl");

        // Test getting a non-existent object
        let result = db.get_object_by_key("nonexistent").await;
        assert!(
            result.is_err(),
            "Should return error for non-existent object"
        );
        assert!(
            matches!(
                result,
                Err(DatabaseError::ObjectNotFound(ref key)) if key == "nonexistent"
            ),
            "Expected ObjectNotFound error with key 'nonexistent', got: {:?}",
            result
        );
    }
}
