package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"incidentlens/backend/internal/detector"
)

type cancelAfterFirstSeedConn struct {
	driver.Conn
	cancel context.CancelFunc
	calls  int
}

func (c *cancelAfterFirstSeedConn) Query(ctx context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.calls++
	if c.calls != 1 {
		return nil, errors.New("second operation query after cancellation")
	}
	return &cancelAfterFirstSeedRows{cancel: c.cancel}, nil
}

type cancelAfterFirstSeedRows struct {
	driver.Rows
	cancel context.CancelFunc
	seen   bool
}

func (r *cancelAfterFirstSeedRows) Next() bool {
	if r.seen {
		return false
	}
	r.seen = true
	return true
}
func (r *cancelAfterFirstSeedRows) Scan(dest ...any) error {
	if len(dest) != 5 {
		return errors.New("unexpected seed columns")
	}
	*dest[0].(*string) = "0123456789abcdef0123456789abcdef"
	*dest[1].(*string) = "0000000000000001"
	*dest[2].(*time.Time) = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	*dest[3].(*uint64) = 10
	*dest[4].(*uint8) = 2
	return nil
}
func (r *cancelAfterFirstSeedRows) Err() error   { return nil }
func (r *cancelAfterFirstSeedRows) Close() error { r.cancel(); return nil }

func TestIncidentEvidenceSeedsRejectsExcessCandidateGroupsBeforeQuery(t *testing.T) {
	candidates := make([]detector.Identity, detector.MaxGroups+1)
	store := &QueryStore{} // No connection: exceeding the declared bound must fail first.
	_, err := store.incidentEvidenceSeeds(context.Background(), time.Time{}, time.Time{}, time.Time{}, candidates)
	if !errors.Is(err, detector.ErrLimit) {
		t.Fatalf("candidate group bound: got %v want detector limit", err)
	}
}

func TestIncidentEvidenceSeedsCancelBetweenOperationsFailsWholeSelection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &cancelAfterFirstSeedConn{cancel: cancel}
	store := &QueryStore{conn: conn}
	from := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	candidates := []detector.Identity{
		{Namespace: "ns", Service: "svc", Operation: "first"},
		{Namespace: "ns", Service: "svc", Operation: "second"},
	}
	seeds, err := store.incidentEvidenceSeeds(ctx, from, from.Add(time.Minute), from.Add(time.Hour), candidates)
	if !errors.Is(err, context.Canceled) || seeds != nil || conn.calls != 1 {
		t.Fatalf("partial seed exposure or extra query: err=%v seeds=%+v queries=%d", err, seeds, conn.calls)
	}
}
