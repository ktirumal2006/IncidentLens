#!/bin/sh
# Bounded, deduplicated storage inspection. No query API exists in Phase 1.
set -eu
cd "$(dirname "$0")/.."
query='SELECT trace_id, groupUniqArray(service_name) AS services, count() AS spans FROM incidentlens.spans FINAL WHERE start_time >= now64(9) - INTERVAL 1 HOUR AND start_time < now64(9) GROUP BY trace_id ORDER BY spans DESC, trace_id LIMIT 20 SETTINGS max_execution_time=5 FORMAT PrettyCompact'
docker compose -f deploy/local/compose.yaml exec -T clickhouse clickhouse-client --password local-admin --query "$query"
# Differences in payload/key for the same logical identity are unsupported.
# Exclude ingested_at, which legitimately changes on replay.
conflicts='SELECT trace_id, span_id, uniqExact(tuple(parent_span_id, service_namespace, service_name, span_name, span_kind, status_code, status_message, start_time, end_time, duration_ns, resource_attributes, span_attributes, scope_attributes, scope_name, scope_version, resource_schema_url, scope_schema_url, events, links, trace_state, trace_flags, dropped_attributes_count, dropped_events_count, dropped_links_count, resource_dropped_attributes_count, scope_dropped_attributes_count)) AS versions FROM incidentlens.spans WHERE start_time >= now64(9) - INTERVAL 1 HOUR AND start_time < now64(9) GROUP BY trace_id, span_id HAVING versions > 1 LIMIT 20 SETTINGS max_execution_time=5 FORMAT PrettyCompact'
docker compose -f deploy/local/compose.yaml exec -T clickhouse clickhouse-client --password local-admin --query "$conflicts"
