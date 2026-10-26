CREATE DATABASE IF NOT EXISTS incidentlens;
CREATE TABLE IF NOT EXISTS incidentlens.spans
(
    trace_id FixedString(32),
    span_id FixedString(16),
    parent_span_id String,
    service_name LowCardinality(String),
    service_namespace LowCardinality(String),
    span_name String,
    span_kind UInt8,
    status_code UInt8,
    status_message String,
    start_time DateTime64(9, 'UTC'),
    end_time DateTime64(9, 'UTC'),
    duration_ns UInt64,
    ingested_at DateTime64(9, 'UTC'),
    resource_attributes String,
    span_attributes String,
    scope_attributes String,
    scope_name String,
    scope_version String,
    resource_schema_url String,
    scope_schema_url String,
    events String,
    links String,
    trace_state String,
    trace_flags UInt32,
    dropped_attributes_count UInt32,
    dropped_events_count UInt32,
    dropped_links_count UInt32,
    resource_dropped_attributes_count UInt32,
    scope_dropped_attributes_count UInt32
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(start_time)
ORDER BY (service_namespace, service_name, start_time, trace_id, span_id)
-- Round deletion eligibility upward: never delete before the exact nanosecond cutoff.
TTL toDateTime(start_time) + INTERVAL 7 DAY + INTERVAL 1 SECOND DELETE;
