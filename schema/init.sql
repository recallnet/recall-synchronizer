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