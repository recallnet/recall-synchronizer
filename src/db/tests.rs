use crate::db::{postgres::PostgresDatabase, Database, DatabaseError, FakeDatabase, ObjectIndex};
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

/// Setup a test database with a specific prefix for test isolation
async fn setup_test_database(db: &(dyn Database + Send + Sync)) -> Vec<ObjectIndex> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut objects = Vec::new();

    // Create 15 objects with different timestamps for comprehensive testing
    for i in 0..15 {
        let modified_at = base_time + Duration::minutes(i * 30);
        let mut object =
            create_test_object_index(&format!("test/object_{:02}.jsonl", i), modified_at);

        // Vary other attributes for realistic testing
        object.size_bytes = Some(1024 * (i + 1));
        object.data_type = match i % 3 {
            0 => "LOGS".to_string(),
            1 => "METRICS".to_string(),
            _ => "EVENTS".to_string(),
        };

        db.add_object(object.clone()).await.unwrap();
        objects.push(object);
    }

    objects
}

// Helper function to create test databases
fn get_test_databases() -> Vec<DatabaseFactory> {
    let mut databases: Vec<DatabaseFactory> = vec![
        // Always include the FakeDatabase
        Box::new(|| {
            Box::pin(async { Box::new(FakeDatabase::new()) as Box<dyn Database + Send + Sync> })
        }),
    ];

    let test_config = load_test_config();

    // Conditionally add the real PostgreSQL implementation when enabled
    if test_config.database.enabled {
        databases.push(Box::new(|| {
            Box::pin(async {
                match get_or_create_postgres_connection().await {
                    Ok(db) => {
                        // Clear test data before returning
                        // Ignore errors as the table might not exist yet
                        let _ = db.clear_data().await;
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
async fn get_objects_with_no_timestamp_filter_returns_all_objects() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let test_objects = setup_test_database(db.as_ref()).await;

        let objects = db.get_objects_to_sync(20, None).await.unwrap();

        assert_eq!(
            objects.len(),
            test_objects.len(),
            "Should return all objects when no timestamp filter is provided. Got {} objects, expected {}",
            objects.len(),
            test_objects.len()
        );
    }
}

#[tokio::test]
async fn get_objects_with_future_timestamp_returns_empty() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let _ = setup_test_database(db.as_ref()).await;

        let future_time = Utc::now() + Duration::days(1);
        let objects = db.get_objects_to_sync(20, Some(future_time)).await.unwrap();

        assert_eq!(
            objects.len(),
            0,
            "Should return no objects when timestamp filter is in the future"
        );
    }
}

#[tokio::test]
async fn get_objects_with_past_timestamp_returns_recent_objects() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let test_objects = setup_test_database(db.as_ref()).await;

        // Use a timestamp that's 5 hours ago (halfway through our test data)
        let midpoint_time = Utc::now() - Duration::hours(5);
        let objects = db
            .get_objects_to_sync(20, Some(midpoint_time))
            .await
            .unwrap();

        // Count objects that should be returned based on our test data
        let expected_count = test_objects
            .iter()
            .filter(|o| o.object_last_modified_at > midpoint_time)
            .count();

        assert_eq!(
            objects.len(),
            expected_count,
            "Should return exactly {} objects modified after the midpoint timestamp",
            expected_count
        );

        // Verify all returned objects are newer than the timestamp
        for obj in &objects {
            assert!(
                obj.object_last_modified_at > midpoint_time,
                "Object {} should be newer than the filter timestamp",
                obj.object_key
            );
        }
    }
}

#[tokio::test]
async fn get_objects_with_limit_at_beginning_of_range_returns_objects() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let _ = setup_test_database(db.as_ref()).await;

        let objects = db.get_objects_to_sync(3, None).await.unwrap();

        assert_eq!(
            objects.len(),
            3,
            "Should return exactly 3 objects when limit is 3"
        );
    }
}

#[tokio::test]
async fn get_objects_with_limit_in_middle_of_range_returns_objects() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let test_objects = setup_test_database(db.as_ref()).await;

        let limit = 8;
        let objects = db
            // Use the second object as a cutoff point
            // This should return the 8 most recent objects after the second one
            .get_objects_to_sync(limit, Some(test_objects[1].object_last_modified_at))
            .await
            .unwrap();

        assert_eq!(
            objects.len(),
            limit as usize,
            "Should return exactly {} objects when limit is {}",
            limit,
            limit
        );

        // Verify we got the most recent objects
        let sorted_test_objects = {
            let mut objs = test_objects[2..].to_vec();
            objs.sort_by(|a, b| b.object_last_modified_at.cmp(&a.object_last_modified_at));
            objs
        };

        for i in 0..limit as usize {
            assert_eq!(
                objects[i].object_key, sorted_test_objects[i].object_key,
                "Objects should be returned in order of most recent first"
            );
        }
    }
}

#[tokio::test]
async fn get_objects_with_limit_beyond_available_records_returns_objects_up_to_last_one() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let test_objects = setup_test_database(db.as_ref()).await;

        let objects = db.get_objects_to_sync(100, None).await.unwrap();

        assert_eq!(
            objects.len(),
            test_objects.len(),
            "Should return all available objects when limit exceeds total count"
        );
    }
}

#[tokio::test]
async fn get_object_by_existing_key_returns_object() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let test_objects = setup_test_database(db.as_ref()).await;

        // Test retrieving an object from the middle of the dataset
        let target_key = "test/object_07.jsonl";
        let object = db.get_object_by_key(target_key).await.unwrap();

        assert_eq!(object.object_key, target_key);

        // Verify it matches our test data
        let expected_object = test_objects
            .iter()
            .find(|o| o.object_key == target_key)
            .unwrap();
        assert_eq!(object.size_bytes, expected_object.size_bytes);
        assert_eq!(object.data_type, expected_object.data_type);
    }
}

#[tokio::test]
async fn get_object_by_nonexistent_key_returns_error() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        let _ = setup_test_database(db.as_ref()).await;

        let result = db.get_object_by_key("nonexistent/object.jsonl").await;

        assert!(
            matches!(
                result,
                Err(DatabaseError::ObjectNotFound(ref key)) if key == "nonexistent/object.jsonl"
            ),
            "Expected ObjectNotFound error for nonexistent key, got: {:?}",
            result
        );
    }
}
