package benchobs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestBoundedConcurrentRecorderAndExplicitOverflow(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ops.jsonl")
	r, err := Open(p, 2048)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Record(Event{Kind: "write", Rows: 192, Success: true, DurationNS: 17})
			}
		}()
	}
	wg.Wait()
	if r.Close() == nil {
		t.Fatal("overflow must invalidate evidence")
	}
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) > 2048 {
		t.Fatal("log exceeded bound")
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	var end struct {
		Kind            string
		Events, Dropped int
		WriteFailed     bool `json:"write_failed"`
	}
	if err = json.Unmarshal([]byte(lines[len(lines)-1]), &end); err != nil {
		t.Fatal(err)
	}
	if end.Kind != "recorder_end" || end.Events+end.Dropped != 400 || end.Dropped == 0 || end.WriteFailed {
		t.Fatalf("bad trailer: %+v", end)
	}
	if _, err = Open(p, 2048); err == nil {
		t.Fatal("must not overwrite evidence")
	}
}

func TestSuccessfulRecorderCountsExactRowsAndTimes(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ops.jsonl")
	r, err := Open(p, 4096)
	if err != nil {
		t.Fatal(err)
	}
	want := Event{Kind: "write", StartedNS: 100, FinishedNS: 119, DurationNS: 19, Rows: 192, Success: true}
	r.Record(want)
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(p)
	var got Event
	if err = json.Unmarshal([]byte(strings.Split(string(b), "\n")[0]), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("changed observation: %+v", got)
	}
	if !strings.Contains(string(b), `"offset":0`) {
		t.Fatal("zero offsets must remain explicit in raw evidence")
	}
}
