-- Optional query-hardening candidate; the frozen baseline retains migration 001.
-- Existing parts remain readable. Materialize explicitly during a planned
-- migration if immediate coverage of retained data is required.
ALTER TABLE incidentlens.spans
    ADD INDEX IF NOT EXISTS trace_id_bloom trace_id
    TYPE bloom_filter(0.001) GRANULARITY 1;
