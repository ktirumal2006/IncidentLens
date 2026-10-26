package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"incidentlens/backend/internal/detector"
	"incidentlens/backend/internal/query"
)

// QueryFailureHeader carries a fixed, bounded cause category. It does not
// expose SQL, identifiers, request parameters, database text, or credentials.
const QueryFailureHeader = "X-IncidentLens-Query-Failure"

func queryFailure(err, contextErr error) (status int, code, message, cause string) {
	var chErr *proto.Exception
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.Is(contextErr, context.DeadlineExceeded):
		return 504, "deadline_exceeded", "query deadline exceeded", "deadline"
	case errors.As(err, &chErr) && chErr.Code == 159:
		return 504, "deadline_exceeded", "query deadline exceeded", "deadline"
	case errors.Is(err, query.ErrInvalid):
		return 400, "invalid_request", "invalid query parameters", ""
	case errors.Is(err, query.ErrNotFound):
		return 404, "not_found", "trace not found", ""
	case errors.Is(err, query.ErrBusy):
		return 503, "unavailable", "query unavailable or resource cap reached", "admission_busy"
	case errors.Is(err, detector.ErrLimit):
		return 503, "unavailable", "query unavailable or resource cap reached", "detector_input_limit"
	case errors.As(err, &chErr):
		return 503, "unavailable", "query unavailable or resource cap reached", "clickhouse_" + strconv.FormatInt(int64(chErr.Code), 10)
	case errors.Is(err, query.ErrTooLarge):
		return 503, "unavailable", "query unavailable or resource cap reached", "resource_cap"
	default:
		return 503, "unavailable", "query unavailable or resource cap reached", "other_query_error"
	}
}

func respondQueryFailure(w http.ResponseWriter, status int, code, message, cause string) {
	if status == 503 || status == 504 {
		w.Header().Set(QueryFailureHeader, cause)
	}
	respondError(w, status, code, message)
}
