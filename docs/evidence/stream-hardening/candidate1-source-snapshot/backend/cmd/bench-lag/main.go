// bench-lag takes bounded, read-only Kafka offset snapshots without a JVM.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type position struct {
	Partition int32 `json:"partition"`
	Committed int64 `json:"committed"`
	End       int64 `json:"end"`
}

func positions(ctx context.Context, c *kgo.Client, topic, group string, count int) ([]position, error) {
	ids := make([]int32, count)
	for i := range ids {
		ids[i] = int32(i)
	}
	f := kmsg.NewPtrOffsetFetchRequest()
	f.Group = group
	f.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: topic, Partitions: ids}}
	f.Groups = []kmsg.OffsetFetchRequestGroup{{Group: group, Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: topic, Partitions: ids}}}}
	response, err := f.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	}
	var parts []kmsg.OffsetFetchResponseGroupTopicPartition
	if len(response.Groups) == 1 {
		g := response.Groups[0]
		if g.Group != group || g.ErrorCode != 0 || len(g.Topics) != 1 || g.Topics[0].Topic != topic {
			return nil, errors.New("invalid group offset response")
		}
		parts = g.Topics[0].Partitions
	} else {
		if len(response.Groups) != 0 || response.ErrorCode != 0 || len(response.Topics) != 1 || response.Topics[0].Topic != topic {
			return nil, errors.New("invalid legacy group offset response")
		}
		for _, p := range response.Topics[0].Partitions {
			parts = append(parts, kmsg.OffsetFetchResponseGroupTopicPartition{Partition: p.Partition, Offset: p.Offset, ErrorCode: p.ErrorCode})
		}
	}
	if len(parts) != count {
		return nil, errors.New("incomplete committed offsets")
	}
	result := make([]position, count)
	seen := make([]bool, count)
	for _, p := range parts {
		if p.Partition < 0 || int(p.Partition) >= count || seen[p.Partition] || p.ErrorCode != 0 {
			return nil, errors.New("invalid committed partition")
		}
		seen[p.Partition] = true
		result[p.Partition] = position{Partition: p.Partition, Committed: p.Offset}
	}
	// Sequential committed then end reads conservatively include concurrent
	// production. They are not an atomic snapshot and retain request time bounds.
	req := kmsg.NewPtrListOffsetsRequest()
	ends := make([]kmsg.ListOffsetsRequestTopicPartition, count)
	for i := range ends {
		ends[i] = kmsg.NewListOffsetsRequestTopicPartition()
		ends[i].Partition = int32(i)
		ends[i].Timestamp = -1
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: topic, Partitions: ends}}
	end, err := req.RequestWith(ctx, c)
	if err != nil {
		return nil, err
	}
	if len(end.Topics) != 1 || end.Topics[0].Topic != topic || len(end.Topics[0].Partitions) != count {
		return nil, errors.New("incomplete end offsets")
	}
	seen = make([]bool, count)
	for _, p := range end.Topics[0].Partitions {
		if p.Partition < 0 || int(p.Partition) >= count || seen[p.Partition] || p.ErrorCode != 0 {
			return nil, errors.New("invalid end partition")
		}
		seen[p.Partition] = true
		result[p.Partition].End = p.Offset
	}
	return result, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	output := flag.String("output", "", "new JSONL file")
	brokers := flag.String("brokers", "127.0.0.1:19092", "Kafka endpoints")
	topic := flag.String("topic", "incidentlens-spans-v1", "observed topic")
	partitions := flag.Int("partitions", 1, "expected partition count (1 or 4)")
	group := flag.String("group", "incidentlens-stream-worker-v1", "consumer group")
	duration := flag.Duration("duration", 15*time.Minute, "hard observation bound (maximum15m)")
	flag.Parse()
	if *output == "" || *duration <= 0 || *duration > 15*time.Minute || (*partitions != 1 && *partitions != 4) {
		return errors.New("output and bounded duration required")
	}
	file, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	client, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(*brokers, ",")...), kgo.FetchMaxBytes(1<<20), kgo.BrokerMaxReadBytes(1<<20))
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()
	encoder := json.NewEncoder(file)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	samples := 0
	for {
		if ctx.Err() != nil {
			break
		}
		started := time.Now()
		attempt, end := context.WithTimeout(ctx, 2*time.Second)
		parts, err := positions(attempt, client, *topic, *group, *partitions)
		end()
		if ctx.Err() != nil {
			break
		}
		value := map[string]any{"kind": "lag", "started_ns": started.UnixNano(), "finished_ns": time.Now().UnixNano(), "duration_ns": time.Since(started).Nanoseconds()}
		if err != nil {
			value["error"] = err.Error()
		} else {
			value["partitions"] = parts
			var committed, latest int64
			valid := true
			for _, p := range parts {
				if p.Committed < 0 || p.End < p.Committed {
					valid = false
				}
				committed += p.Committed
				latest += p.End
			}
			value["committed"], value["end"] = committed, latest
			if valid {
				value["lag_records"] = latest - committed
			} else {
				value["uninitialized_or_inconsistent"] = true
			}

		}
		if err = encoder.Encode(value); err != nil {
			return err
		}
		samples++
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
	return encoder.Encode(map[string]any{"kind": "observer_end", "samples": samples, "finished_ns": time.Now().UnixNano()})
}
