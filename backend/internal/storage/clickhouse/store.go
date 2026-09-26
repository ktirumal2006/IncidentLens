// Package clickhouse stores completed, normalized spans synchronously.
package clickhouse

import (
	"context"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"incidentlens/backend/internal/trace"
)

type Store struct{ conn driver.Conn }

func Open(address, user, password, database string) (*Store, error) {
	conn, err := ch.Open(&ch.Options{
		Addr:        []string{address},
		Auth:        ch.Auth{Database: database, Username: user, Password: password},
		DialTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second,
		MaxOpenConns: 4, MaxIdleConns: 4,
		Settings: ch.Settings{"async_insert": 0, "max_execution_time": 5},
	})
	if err != nil {
		return nil, err
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Ping(ctx context.Context) error { return s.conn.Ping(ctx) }
func (s *Store) Close() error                   { return s.conn.Close() }

const insertSQL = `INSERT INTO spans (
trace_id, span_id, parent_span_id, service_name, service_namespace,
span_name, span_kind, status_code, status_message, start_time, end_time,
duration_ns, ingested_at, resource_attributes, span_attributes, scope_attributes,
scope_name, scope_version, resource_schema_url, scope_schema_url, events, links,
trace_state, trace_flags, dropped_attributes_count, dropped_events_count,
dropped_links_count, resource_dropped_attributes_count, scope_dropped_attributes_count)`

// Write sends one bounded batch. Send must complete before callers acknowledge it.
// A failed Send may have committed data: callers must assume replay is possible.
func (s *Store) Write(ctx context.Context, rows []trace.Row) error {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) > 256 {
		return fmt.Errorf("batch exceeds 256 spans")
	}
	batch, err := s.conn.PrepareBatch(ctx, insertSQL)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, r := range rows {
		if err := batch.Append(r.TraceID, r.SpanID, r.ParentSpanID,
			r.ServiceName, r.ServiceNamespace, r.SpanName, r.SpanKind,
			r.StatusCode, r.StatusMessage, r.StartTime, r.EndTime, r.DurationNS,
			r.IngestedAt, r.ResourceAttributes, r.SpanAttributes, r.ScopeAttributes,
			r.ScopeName, r.ScopeVersion, r.ResourceSchemaURL, r.ScopeSchemaURL,
			r.Events, r.Links, r.TraceState, r.TraceFlags, r.DroppedAttributesCount,
			r.DroppedEventsCount, r.DroppedLinksCount, r.ResourceDroppedAttributesCount,
			r.ScopeDroppedAttributesCount); err != nil {
			return err
		}
	}
	return batch.Send()
}
