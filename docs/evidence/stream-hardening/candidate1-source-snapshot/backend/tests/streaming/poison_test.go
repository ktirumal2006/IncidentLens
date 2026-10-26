package streaming

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"incidentlens/backend/internal/stream"
	"incidentlens/backend/internal/trace"
)

type forbiddenWriter struct{ called bool }

func (w *forbiddenWriter) Write(context.Context, []trace.Row) error {
	w.called = true
	return errors.New("unexpected storage write")
}

func isolatedTopic(t *testing.T) (*kgo.Client, string) {
	t.Helper()
	requireStack(t)
	c, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:19092"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	topic := "streamtest-" + seed(t)
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10000
	v := kmsg.NewCreateTopicsRequestTopic()
	v.Topic = topic
	v.NumPartitions = 1
	v.ReplicationFactor = 1
	v.Configs = []kmsg.CreateTopicsRequestTopicConfig{{Name: "retention.ms", Value: kmsg.StringPtr("3600000")}, {Name: "retention.bytes", Value: kmsg.StringPtr("16777216")}}
	req.Topics = []kmsg.CreateTopicsRequestTopic{v}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Topics) != 1 || res.Topics[0].ErrorCode != 0 {
		t.Fatalf("topic creation: %+v", res)
	}
	t.Logf("isolated topic %s retained for inspection, retention 1h/16MiB", topic)
	return c, topic
}
func commitAt(t *testing.T, c *kgo.Client, topic, group string, offset int64) {
	t.Helper()
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition = 0
	p.Offset = offset
	req.Topics = []kmsg.OffsetCommitRequestTopic{{Topic: topic, Partitions: []kmsg.OffsetCommitRequestTopicPartition{p}}}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit: %+v", res)
	}
}
func committedAt(t *testing.T, c *kgo.Client, topic, group string) int64 {
	t.Helper()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: []int32{0}}}
	g := kmsg.NewOffsetFetchRequestGroup()
	g.Group = group
	g.Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: []int32{0}}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{g}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	res, err := req.RequestWith(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Groups) == 1 {
		g := res.Groups[0]
		if g.ErrorCode != 0 || len(g.Topics) != 1 || len(g.Topics[0].Partitions) != 1 || g.Topics[0].Partitions[0].ErrorCode != 0 {
			t.Fatalf("fetch offset: %+v", res)
		}
		return g.Topics[0].Partitions[0].Offset
	}
	if res.ErrorCode != 0 || len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("fetch offset: %+v", res)
	}
	return res.Topics[0].Partitions[0].Offset
}
func TestPoisonRecordDoesNotAdvanceOffset(t *testing.T) {
	for name, payload := range map[string][]byte{"malformed": []byte(`{`), "unsupported": []byte(`{"version":999,"rows":[{}]}`)} {
		t.Run(name, func(t *testing.T) {
			c, topic := isolatedTopic(t)
			group := topic + "-group"
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := c.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: payload}).FirstErr(); err != nil {
				t.Fatal(err)
			}
			commitAt(t, c, topic, group, 0)
			writer := &forbiddenWriter{}
			worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writer)
			if err != nil {
				t.Fatal(err)
			}
			runCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
			err = worker.Run(runCtx)
			stop()
			worker.Close()
			if !errors.Is(err, stream.ErrInvalidRecord) || writer.called {
				t.Fatalf("poison handling: err=%v wrote=%v", err, writer.called)
			}
			if got := committedAt(t, c, topic, group); got != 0 {
				t.Fatalf("poison advanced offset to %d", got)
			}
		})
	}
}
func TestRetainedOffsetGapFailsWithoutReset(t *testing.T) {
	c, topic := isolatedTopic(t)
	group := topic + "-group"
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := c.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte(`{"version":999}`)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	commitAt(t, c, topic, group, 0)
	req := kmsg.NewPtrDeleteRecordsRequest()
	req.TimeoutMillis = 10000
	req.Topics = []kmsg.DeleteRecordsRequestTopic{{Topic: topic, Partitions: []kmsg.DeleteRecordsRequestTopicPartition{{Partition: 0, Offset: 1}}}}
	res, err := req.RequestWith(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Topics) != 1 || len(res.Topics[0].Partitions) != 1 || res.Topics[0].Partitions[0].ErrorCode != 0 || res.Topics[0].Partitions[0].LowWatermark < 1 {
		t.Fatalf("fixture truncation failed: %+v", res)
	}
	writer := &forbiddenWriter{}
	worker, err := stream.NewWorker([]string{"127.0.0.1:19092"}, topic, group, writer)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	err = worker.Run(runCtx)
	stop()
	worker.Close()
	if !errors.Is(err, kerr.OffsetOutOfRange) || writer.called {
		t.Fatalf("gap silently reset or wrong failure: err=%v wrote=%v", err, writer.called)
	}
	if got := committedAt(t, c, topic, group); got != 0 {
		t.Fatalf("gap advanced offset to %d", got)
	}
}
