use crate::db::{postgres::PostgresDatabase, Database, DatabaseError, FakeDatabase, ObjectIndex};
use crate::test_utils::{create_test_object_index, load_test_config};
use chrono::{Duration, Utc};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

// Type alias to simplify the complex type for database factory functions
type DatabaseFactory =
    Box<dyn Fn() -> futures::future::BoxFuture<'static, Box<dyn Database + Send + Sync>>>;

// Counter for generating unique schema names
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique schema name for test isolation
fn generate_test_schema() -> String {
    let count = SCHEMA_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    format!("test_{}_{}", timestamp, count)
}

/// Creates a new PostgreSQL database connection with a unique schema for test isolation
async fn create_postgres_with_schema() -> Result<Arc<PostgresDatabase>, String> {
    // Get database URL from test config
    let test_config = load_test_config();
    let db_url = test_config.database.url;
    let schema = generate_test_schema();

    let pg_db = PostgresDatabase::new_with_schema(&db_url, Some(schema))
        .await
        .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

    Ok(Arc::new(pg_db))
}

/// Setup a test database with a specific prefix for test isolation
async fn setup_test_database(db: &(dyn Database + Send + Sync)) -> Vec<ObjectIndex> {
    setup_test_database_with_prefix(db, "test").await
}

/// Setup a test database with a custom prefix for better test isolation
async fn setup_test_database_with_prefix(
    db: &(dyn Database + Send + Sync),
    prefix: &str,
) -> Vec<ObjectIndex> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut objects = Vec::new();

    // Create 15 objects with different timestamps for comprehensive testing
    for i in 0..15 {
        let modified_at = base_time + Duration::minutes(i * 30);
        let mut object =
            create_test_object_index(&format!("{}/object_{:02}.jsonl", prefix, i), modified_at);

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
                match create_postgres_with_schema().await {
                    Ok(db) => Box::new(db) as Box<dyn Database + Send + Sync>,
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

        let objects = db
            .get_objects_to_sync_with_id(20, None, None)
            .await
            .unwrap();

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
        let objects = db
            .get_objects_to_sync_with_id(20, Some(future_time), None)
            .await
            .unwrap();

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
            .get_objects_to_sync_with_id(20, Some(midpoint_time), None)
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

        let objects = db.get_objects_to_sync_with_id(3, None, None).await.unwrap();

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
            .get_objects_to_sync_with_id(limit, Some(test_objects[1].object_last_modified_at), None)
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

        let objects = db
            .get_objects_to_sync_with_id(100, None, None)
            .await
            .unwrap();

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

#[tokio::test]
async fn get_objects_with_same_timestamp_paginate_by_id() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        
        // Create objects with the same timestamp to test ID-based ordering
        let shared_timestamp = Utc::now() - Duration::hours(5);
        let mut objects_same_time = Vec::new();
        
        // Create 7 objects with the exact same timestamp
        for i in 0..7 {
            let mut object = create_test_object_index(
                &format!("test/same_time_{:02}.jsonl", i),
                shared_timestamp,
            );
            // Ensure each object has a unique ID
            object.id = uuid::Uuid::new_v4();
            db.add_object(object.clone()).await.unwrap();
            objects_same_time.push(object);
        }
        
        // Sort objects by ID to match expected database behavior
        objects_same_time.sort_by(|a, b| a.id.cmp(&b.id));
        
        // Get first batch without after_id
        let batch1 = db.get_objects_to_sync_with_id(3, None, None).await.unwrap();
        
        assert_eq!(batch1.len(), 3, "First batch should contain 3 objects");
        
        // Verify the objects are sorted by ID when timestamps are equal
        for i in 0..3 {
            assert_eq!(
                batch1[i].id, objects_same_time[i].id,
                "Objects should be returned in ID order"
            );
        }
        
        // Get second batch using after_id from first batch
        let last_id_batch1 = batch1.last().unwrap().id;
        let batch2 = db
            .get_objects_to_sync_with_id(3, Some(shared_timestamp), Some(last_id_batch1))
            .await
            .unwrap();
        
        assert_eq!(batch2.len(), 3, "Second batch should contain 3 objects");
        
        // Verify we got the next 3 objects in ID order
        for i in 0..3 {
            assert_eq!(
                batch2[i].id,
                objects_same_time[i + 3].id,
                "Second batch should continue from where first batch ended"
            );
        }
        
        // Get third batch (should only have 1 remaining object)
        let last_id_batch2 = batch2.last().unwrap().id;
        let batch3 = db
            .get_objects_to_sync_with_id(3, Some(shared_timestamp), Some(last_id_batch2))
            .await
            .unwrap();
        
        assert_eq!(
            batch3.len(),
            1,
            "Third batch should contain only 1 remaining object"
        );
        assert_eq!(
            batch3[0].id, objects_same_time[6].id,
            "Third batch should contain the last object"
        );
        
        // Verify no more objects after the last one
        let last_id_batch3 = batch3.last().unwrap().id;
        let batch4 = db
            .get_objects_to_sync_with_id(3, Some(shared_timestamp), Some(last_id_batch3))
            .await
            .unwrap();
        
        assert_eq!(
            batch4.len(),
            0,
            "No more objects should be returned after the last ID"
        );
    }
}

#[tokio::test]
async fn get_objects_with_mixed_timestamps_and_after_id() {
    for db_factory in get_test_databases() {
        let db = db_factory().await;
        
        // Create objects with the same timestamp
        let shared_timestamp = Utc::now() - Duration::hours(5);
        let mut objects_same_time = Vec::new();
        
        for i in 0..5 {
            let mut object = create_test_object_index(
                &format!("test/same_time_{:02}.jsonl", i),
                shared_timestamp,
            );
            object.id = uuid::Uuid::new_v4();
            db.add_object(object.clone()).await.unwrap();
            objects_same_time.push(object);
        }
        
        // Sort objects by ID to match expected database behavior
        objects_same_time.sort_by(|a, b| a.id.cmp(&b.id));
        
        // Add objects with different timestamps
        let newer_timestamp = shared_timestamp + Duration::hours(1);
        let older_timestamp = shared_timestamp - Duration::hours(1);
        
        let mut newer_object = create_test_object_index("test/newer.jsonl", newer_timestamp);
        newer_object.id = uuid::Uuid::new_v4();
        db.add_object(newer_object.clone()).await.unwrap();
        
        let mut older_object = create_test_object_index("test/older.jsonl", older_timestamp);
        older_object.id = uuid::Uuid::new_v4();
        db.add_object(older_object).await.unwrap();
        
        // Test 1: Verify timestamp + ID filtering works correctly
        let mixed_batch = db
            .get_objects_to_sync_with_id(10, Some(shared_timestamp), Some(objects_same_time[2].id))
            .await
            .unwrap();
        
        // Should get: remaining objects with shared_timestamp AND id > objects_same_time[2].id
        // PLUS the newer object (but NOT the older object)
        let expected_count = 2 + 1; // 2 remaining same-timestamp objects + 1 newer object
        assert_eq!(
            mixed_batch.len(),
            expected_count,
            "Should return objects with same timestamp and greater ID, plus all newer objects"
        );
        
        // Verify the newer timestamp object comes first (sorted by timestamp DESC)
        assert_eq!(
            mixed_batch[0].id, newer_object.id,
            "Newer timestamp object should come first"
        );
        
        // Verify we got the right same-timestamp objects
        assert_eq!(
            mixed_batch[1].id, objects_same_time[3].id,
            "Should get object at index 3"
        );
        assert_eq!(
            mixed_batch[2].id, objects_same_time[4].id,
            "Should get object at index 4"
        );
        
        // Test 2: Verify deterministic ordering by running the same query multiple times
        for _ in 0..3 {
            let consistent_batch = db
                .get_objects_to_sync_with_id(
                    5,
                    Some(shared_timestamp),
                    Some(objects_same_time[1].id),
                )
                .await
                .unwrap();
            
            // Should always get the same objects in the same order
            assert_eq!(consistent_batch.len(), 4); // 1 newer + 3 same-timestamp objects
            assert_eq!(consistent_batch[0].id, newer_object.id);
            for i in 0..3 {
                assert_eq!(
                    consistent_batch[i + 1].id,
                    objects_same_time[i + 2].id,
                    "Same-timestamp objects should be in consistent ID order"
                );
            }
        }
    }
}
