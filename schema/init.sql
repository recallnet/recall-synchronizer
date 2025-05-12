-- Create the object_index table
CREATE TABLE object_index (
    id UUID PRIMARY KEY,
    object_key TEXT UNIQUE NOT NULL,
    bucket_name VARCHAR(100) NOT NULL,
    competition_id UUID,
    agent_id UUID,
    data_type VARCHAR(50) NOT NULL,
    size_bytes BIGINT,
    content_hash VARCHAR(128),
    metadata JSONB,
    event_timestamp TIMESTAMPTZ,
    object_last_modified_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Create an index for querying by last_modified_at
CREATE INDEX object_index_last_modified_idx ON object_index (object_last_modified_at);

-- Insert sample test data
INSERT INTO object_index (
    id, 
    object_key, 
    bucket_name, 
    competition_id, 
    agent_id, 
    data_type, 
    size_bytes, 
    content_hash,
    metadata,
    event_timestamp,
    object_last_modified_at,
    created_at,
    updated_at
) VALUES (
    gen_random_uuid(),
    'test-competition/test-agent/logs/2025-05-01T12_00_00Z.jsonl',
    'competition-data',
    gen_random_uuid(),
    gen_random_uuid(),
    'LOG',
    1024,
    'abcdef1234567890',
    '{"format": "jsonl", "compression": null}',
    '2025-05-01T12:00:00Z',
    '2025-05-01T12:05:00Z',
    '2025-05-01T12:05:00Z',
    '2025-05-01T12:05:00Z'
);

-- Insert another test record with different timestamp
INSERT INTO object_index (
    id, 
    object_key, 
    bucket_name, 
    competition_id, 
    agent_id, 
    data_type, 
    size_bytes, 
    content_hash,
    metadata,
    event_timestamp,
    object_last_modified_at,
    created_at,
    updated_at
) VALUES (
    gen_random_uuid(),
    'test-competition/test-agent/results/2025-05-01T12_30_00Z.jsonl',
    'competition-data',
    gen_random_uuid(),
    gen_random_uuid(),
    'RESULT',
    512,
    '0123456789abcdef',
    '{"format": "jsonl", "compression": null}',
    '2025-05-01T12:30:00Z',
    '2025-05-01T12:35:00Z',
    '2025-05-01T12:35:00Z',
    '2025-05-01T12:35:00Z'
);