use crate::db::{
    pg_schema::SchemaMode, postgres::PostgresDatabase, Database, FakeDatabase, ObjectIndex,
};
use crate::test_utils::{
    create_test_object_index_direct, create_test_object_index_s3, is_db_enabled, load_test_config,
};
use chrono::{Duration, Utc};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

// Enum to represent different storage modes for testing
#[derive(Clone, Copy, Debug, PartialEq)]
enum StorageMode {
    S3,
    Direct,
}

// Type alias for database factory with storage mode
type DatabaseFactory = Box<
    dyn Fn() -> futures::future::BoxFuture<'static, (Box<dyn Database + Send + Sync>, StorageMode)>,
>;

// Counter for generating unique schema names
static SCHEMA_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Generate a unique schema name for test isolation
fn generate_test_schema() -> String {
    let count = SCHEMA_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap();
    format!("test_{}_{}", timestamp, count)
}

/// Creates a new PostgreSQL database connection with a unique schema for test isolation (S3 mode)
async fn create_postgres_s3_with_schema() -> Result<Arc<PostgresDatabase>, String> {
    let config = load_test_config().map_err(|e| format!("Failed to load test config: {}", e))?;
    let db_url = config.database.url;
    let schema = generate_test_schema();

    let pg_db = PostgresDatabase::new_with_schema(&db_url, Some(schema), SchemaMode::S3)
        .await
        .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

    Ok(Arc::new(pg_db))
}

/// Creates a new PostgreSQL database connection with a unique schema for test isolation (Direct mode)
async fn create_postgres_direct_with_schema() -> Result<Arc<PostgresDatabase>, String> {
    let config = load_test_config().map_err(|e| format!("Failed to load test config: {}", e))?;
    let db_url = config.database.url;
    let schema = generate_test_schema();

    let pg_db = PostgresDatabase::new_with_schema(&db_url, Some(schema), SchemaMode::Direct)
        .await
        .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))?;

    Ok(Arc::new(pg_db))
}

/// Setup test objects based on storage mode
async fn add_test_objects(
    db: &(dyn Database + Send + Sync),
    mode: StorageMode,
) -> Vec<ObjectIndex> {
    match mode {
        StorageMode::S3 => add_test_objects_s3(db).await,
        StorageMode::Direct => add_test_objects_direct(db).await,
    }
}

/// Setup a test database with S3 mode objects
async fn add_test_objects_s3(db: &(dyn Database + Send + Sync)) -> Vec<ObjectIndex> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut objects = Vec::new();

    // Create 15 objects with different timestamps for comprehensive testing
    for i in 0..15 {
        let modified_at = base_time + Duration::minutes(i * 30);
        let mut object =
            create_test_object_index_s3(&format!("test/object_{:02}.jsonl", i), modified_at);

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

/// Setup a test database with Direct mode objects
async fn add_test_objects_direct(db: &(dyn Database + Send + Sync)) -> Vec<ObjectIndex> {
    let base_time = Utc::now() - Duration::hours(10);
    let mut objects = Vec::new();
    let competition_id = Uuid::new_v4();
    let agent_id = Uuid::new_v4();

    // Create 15 objects with different timestamps for comprehensive testing
    for i in 0..15 {
        let modified_at = base_time + Duration::minutes(i * 30);
        let data = format!("Test data for object {}", i).into_bytes();
        let mut object =
            create_test_object_index_direct(competition_id.to_string(), agent_id.to_string(), data);

        object.created_at = modified_at;
        object.event_timestamp = Some(modified_at);

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

// Helper function to create all test databases with their storage modes
fn get_all_test_databases() -> Vec<DatabaseFactory> {
    let mut databases: Vec<DatabaseFactory> = vec![
        Box::new(|| {
            Box::pin(async {
                (
                    Box::new(FakeDatabase::new()) as Box<dyn Database + Send + Sync>,
                    StorageMode::S3,
                )
            })
        }),
        Box::new(|| {
            Box::pin(async {
                (
                    Box::new(FakeDatabase::new()) as Box<dyn Database + Send + Sync>,
                    StorageMode::Direct,
                )
            })
        }),
    ];

    if is_db_enabled() {
        databases.push(Box::new(|| {
            Box::pin(async {
                match create_postgres_s3_with_schema().await {
                    Ok(db) => (Box::new(db) as Box<dyn Database + Send + Sync>, StorageMode::S3),
                    Err(e) => {
                        panic!(
                            "Failed to connect to PostgreSQL: {}. Set database.enabled=false in config.toml to skip these tests.",
                            e
                        );
                    }
                }
            })
        }));

        databases.push(Box::new(|| {
            Box::pin(async {
                match create_postgres_direct_with_schema().await {
                    Ok(db) => (Box::new(db) as Box<dyn Database + Send + Sync>, StorageMode::Direct),
                    Err(e) => {
                        panic!(
                            "Failed to connect to PostgreSQL: {}. Set database.enabled=false in config.toml to skip these tests.",
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
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;

        let test_objects = add_test_objects(&*db, mode).await;
        let objects = db.get_objects(20, None, None).await.unwrap();

        assert_eq!(
            objects.len(),
            test_objects.len(),
            "Should return all objects when no timestamp filter is provided. Got {} objects, expected {}",
            objects.len(),
            test_objects.len()
        );

        if mode == StorageMode::Direct {
            for obj in &objects {
                assert!(obj.data.is_some(), "Direct objects should have data");
            }
        }
    }
}

#[tokio::test]
async fn get_objects_with_future_timestamp_returns_empty() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;

        let _ = add_test_objects(&*db, mode).await;
        let future_time = Utc::now() + Duration::days(1);
        let objects = db.get_objects(20, Some(future_time), None).await.unwrap();

        assert_eq!(
            objects.len(),
            0,
            "Should return no objects when timestamp filter is in the future"
        );
    }
}

#[tokio::test]
async fn get_objects_with_past_timestamp_returns_recent_objects() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;
        let midpoint_time = Utc::now() - Duration::hours(5);

        let test_objects = add_test_objects(&*db, mode).await;
        let objects = db.get_objects(20, Some(midpoint_time), None).await.unwrap();

        let expected_count = test_objects
            .iter()
            .filter(|o| o.created_at > midpoint_time)
            .count();

        assert_eq!(
            objects.len(),
            expected_count,
            "Should return exactly {} objects modified after the midpoint timestamp",
            expected_count
        );

        for obj in &objects {
            assert!(
                obj.created_at > midpoint_time,
                "Object {} should be newer than the filter timestamp",
                obj.id
            );

            if mode == StorageMode::Direct {
                assert!(obj.data.is_some(), "Direct objects should have data");
            }
        }
    }
}

#[tokio::test]
async fn get_objects_with_limit_at_beginning_of_range_returns_objects() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;

        let _ = add_test_objects(&*db, mode).await;
        let objects = db.get_objects(3, None, None).await.unwrap();
        assert_eq!(
            objects.len(),
            3,
            "Should return exactly 3 objects when limit is 3"
        );

        if mode == StorageMode::Direct {
            for obj in &objects {
                assert!(obj.data.is_some(), "Direct objects should have data");
            }
        }
    }
}

#[tokio::test]
async fn get_objects_with_limit_in_middle_of_range_returns_objects() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;
        let limit = 8;

        let test_objects = add_test_objects(&*db, mode).await;
        let objects = db
            .get_objects(limit, Some(test_objects[1].created_at), None)
            .await
            .unwrap();

        assert_eq!(objects.len(), limit as usize);

        let sorted_test_objects = {
            let mut objs = test_objects[2..].to_vec();
            objs.sort_by(|a, b| a.created_at.cmp(&b.created_at));
            objs
        };

        for i in 0..limit as usize {
            assert_eq!(
                objects[i].id, sorted_test_objects[i].id,
                "Objects should be returned in order of oldest first"
            );
        }
    }
}

#[tokio::test]
async fn get_objects_with_limit_beyond_available_records_returns_objects_up_to_last_one() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;
        let test_objects = add_test_objects(&*db, mode).await;

        let objects = db.get_objects(100, None, None).await.unwrap();

        assert_eq!(
            objects.len(),
            test_objects.len(),
            "Should return all available objects when limit exceeds total count"
        );
    }
}

#[tokio::test]
async fn get_objects_with_same_timestamp_and_after_id_should_paginate() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;

        let shared_timestamp = Utc::now() - Duration::hours(5);
        let mut objects_same_time = Vec::new();

        for i in 0..7 {
            let mut object = match mode {
                StorageMode::S3 => create_test_object_index_s3(
                    &format!("test/same_time_{:02}.jsonl", i),
                    shared_timestamp,
                ),
                StorageMode::Direct => {
                    let mut obj = create_test_object_index_direct(
                        Uuid::new_v4().to_string(),
                        Uuid::new_v4().to_string(),
                        format!("Test data {}", i).into_bytes(),
                    );
                    obj.created_at = shared_timestamp;
                    obj
                }
            };
            object.id = uuid::Uuid::new_v4();
            db.add_object(object.clone()).await.unwrap();
            objects_same_time.push(object);
        }

        objects_same_time.sort_by(|a, b| a.id.cmp(&b.id));

        // Get first batch without after_id
        let batch1 = db.get_objects(3, None, None).await.unwrap();

        assert_eq!(batch1.len(), 3, "First batch should contain 3 objects");

        for i in 0..3 {
            assert_eq!(
                batch1[i].id, objects_same_time[i].id,
                "Objects should be returned in ID order"
            );
        }

        // Get second batch using after_id from first batch
        let last_id_batch1 = batch1.last().unwrap().id;
        let batch2 = db
            .get_objects(3, Some(shared_timestamp), Some(last_id_batch1))
            .await
            .unwrap();

        assert_eq!(batch2.len(), 3, "Second batch should contain 3 objects");

        for i in 0..3 {
            assert_eq!(
                batch2[i].id,
                objects_same_time[i + 3].id,
                "Second batch should continue from where first batch ended"
            );
        }

        let last_id_batch2 = batch2.last().unwrap().id;
        let batch3 = db
            .get_objects(3, Some(shared_timestamp), Some(last_id_batch2))
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

        let last_id_batch3 = batch3.last().unwrap().id;
        let batch4 = db
            .get_objects(3, Some(shared_timestamp), Some(last_id_batch3))
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
async fn get_objects_with_mixed_timestamps_and_after_id_filters_correctly() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;

        let shared_timestamp = Utc::now() - Duration::hours(5);
        let mut objects_same_time = Vec::new();

        for i in 0..5 {
            let mut object = match mode {
                StorageMode::S3 => create_test_object_index_s3(
                    &format!("test/same_time_{:02}.jsonl", i),
                    shared_timestamp,
                ),
                StorageMode::Direct => {
                    let mut obj = create_test_object_index_direct(
                        Uuid::new_v4().to_string(),
                        Uuid::new_v4().to_string(),
                        format!("Test data {}", i).into_bytes(),
                    );
                    obj.created_at = shared_timestamp;
                    obj
                }
            };
            object.id = uuid::Uuid::new_v4();
            db.add_object(object.clone()).await.unwrap();
            objects_same_time.push(object);
        }

        objects_same_time.sort_by(|a, b| a.id.cmp(&b.id));

        let newer_timestamp = shared_timestamp + Duration::hours(1);
        let older_timestamp = shared_timestamp - Duration::hours(1);

        let mut newer_object = match mode {
            StorageMode::S3 => create_test_object_index_s3("test/newer.jsonl", newer_timestamp),
            StorageMode::Direct => {
                let mut obj = create_test_object_index_direct(
                    Uuid::new_v4().to_string(),
                    Uuid::new_v4().to_string(),
                    b"Newer test data".to_vec(),
                );
                obj.created_at = newer_timestamp;
                obj
            }
        };
        newer_object.id = uuid::Uuid::new_v4();
        db.add_object(newer_object.clone()).await.unwrap();

        let mut older_object = match mode {
            StorageMode::S3 => create_test_object_index_s3("test/older.jsonl", older_timestamp),
            StorageMode::Direct => {
                let mut obj = create_test_object_index_direct(
                    Uuid::new_v4().to_string(),
                    Uuid::new_v4().to_string(),
                    b"Older test data".to_vec(),
                );
                obj.created_at = older_timestamp;
                obj
            }
        };
        older_object.id = uuid::Uuid::new_v4();
        db.add_object(older_object).await.unwrap();

        let mixed_batch = db
            .get_objects(10, Some(shared_timestamp), Some(objects_same_time[2].id))
            .await
            .unwrap();

        let expected_count = 2 + 1; // 2 remaining same-timestamp objects + 1 newer object
        assert_eq!(
            mixed_batch.len(),
            expected_count,
            "Should return objects with same timestamp and greater ID, plus all newer objects"
        );

        assert_eq!(
            mixed_batch[0].id, objects_same_time[3].id,
            "Should get object at index 3 first"
        );
        assert_eq!(
            mixed_batch[1].id, objects_same_time[4].id,
            "Should get object at index 4 second"
        );

        assert_eq!(
            mixed_batch[2].id, newer_object.id,
            "Newer timestamp object should come last"
        );
    }
}

#[tokio::test]
async fn get_objects_with_after_id_returns_with_consistent_ordering() {
    for db_factory in get_all_test_databases() {
        let (db, mode) = db_factory().await;

        let shared_timestamp = Utc::now() - Duration::hours(5);
        let mut objects_same_time = Vec::new();

        // Create 5 objects with the same timestamp
        for i in 0..5 {
            let mut object = match mode {
                StorageMode::S3 => create_test_object_index_s3(
                    &format!("test/same_time_{:02}.jsonl", i),
                    shared_timestamp,
                ),
                StorageMode::Direct => {
                    let mut obj = create_test_object_index_direct(
                        Uuid::new_v4().to_string(),
                        Uuid::new_v4().to_string(),
                        format!("Test data {}", i).into_bytes(),
                    );
                    obj.created_at = shared_timestamp;
                    obj
                }
            };
            object.id = uuid::Uuid::new_v4();
            db.add_object(object.clone()).await.unwrap();
            objects_same_time.push(object);
        }

        objects_same_time.sort_by(|a, b| a.id.cmp(&b.id));

        let newer_timestamp = shared_timestamp + Duration::hours(1);
        let mut newer_object = match mode {
            StorageMode::S3 => create_test_object_index_s3("test/newer.jsonl", newer_timestamp),
            StorageMode::Direct => {
                let mut obj = create_test_object_index_direct(
                    Uuid::new_v4().to_string(),
                    Uuid::new_v4().to_string(),
                    b"Newer test data".to_vec(),
                );
                obj.created_at = newer_timestamp;
                obj
            }
        };
        newer_object.id = uuid::Uuid::new_v4();
        db.add_object(newer_object.clone()).await.unwrap();

        // Verify deterministic ordering by running the same query multiple times
        for run in 0..3 {
            let consistent_batch = db
                .get_objects(5, Some(shared_timestamp), Some(objects_same_time[1].id))
                .await
                .unwrap();

            assert_eq!(
                consistent_batch.len(),
                4,
                "Run {}: Should return 3 same-timestamp objects + 1 newer",
                run + 1
            );

            for i in 0..3 {
                assert_eq!(
                    consistent_batch[i].id,
                    objects_same_time[i + 2].id,
                    "Run {}: Same-timestamp objects should be in consistent ID order at position {}",
                    run + 1,
                    i
                );
            }

            assert_eq!(
                consistent_batch[3].id,
                newer_object.id,
                "Run {}: Newer object should always be last",
                run + 1
            );
        }
    }
}

// Competition ID filtering tests removed - filtering is now done at the application level

// Test removed - competition ID filtering is now done at the application level
