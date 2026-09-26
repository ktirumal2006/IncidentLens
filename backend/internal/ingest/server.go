// Package ingest implements the trace-only OTLP acknowledgement boundary.
package ingest

import (
	"context"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	// Register gzip for the Collector default OTLP export encoding.
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"incidentlens/backend/internal/trace"
)

const (
	BatchSize             = 256
	MaxConcurrentRequests = 4
	WriteTimeout          = 5 * time.Second
)

type Writer interface {
	Write(context.Context, []trace.Row) error
}
type Server struct {
	collector.UnimplementedTraceServiceServer
	writer Writer
	slots  chan struct{}
	now    func() time.Time
}

func New(writer Writer) *Server {
	return &Server{writer: writer, slots: make(chan struct{}, MaxConcurrentRequests), now: time.Now}
}
func NewGRPCServer(writer Writer) *grpc.Server {
	server := grpc.NewServer(grpc.MaxRecvMsgSize(trace.MaxRequestBytes))
	collector.RegisterTraceServiceServer(server, New(writer))
	return server
}
func (s *Server) Export(ctx context.Context, req *collector.ExportTraceServiceRequest) (*collector.ExportTraceServiceResponse, error) {
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, status.Error(codes.Unavailable, "ingestion capacity exhausted")
	}
	ctx, cancel := context.WithTimeout(ctx, WriteTimeout)
	defer cancel()
	rows, rejected, err := trace.Normalize(req, s.now())
	if err != nil {
		return nil, status.Error(codes.ResourceExhausted, "request exceeds ingestion limit")
	}
	for start := 0; start < len(rows); start += BatchSize {
		end := start + BatchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := s.writer.Write(ctx, rows[start:end]); err != nil {
			return nil, status.Error(codes.Unavailable, "storage write did not complete; retry may duplicate spans")
		}
		if ctx.Err() != nil {
			return nil, status.Error(codes.Unavailable, "storage acknowledgement deadline exceeded")
		}
	}
	response := &collector.ExportTraceServiceResponse{}
	if rejected > 0 {
		response.PartialSuccess = &collector.ExportTracePartialSuccess{RejectedSpans: rejected, ErrorMessage: "spans rejected by validation or payload policy"}
	}
	return response, nil
}
