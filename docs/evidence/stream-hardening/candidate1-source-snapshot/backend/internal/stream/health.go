package stream

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CheckTopic checks the declared topic shape without creating it. Only the
// explicit retained v1 topic and isolated acceptance topics use one partition.
func CheckTopic(ctx context.Context, client *kgo.Client, topic string) error {
	if ExpectedPartitions(topic) == 0 {
		return fmt.Errorf("unsupported stream topic %q", topic)
	}
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topic)}}
	req.AllowAutoTopicCreation = false
	response, err := client.Request(ctx, req)
	if err != nil {
		return err
	}
	meta, ok := response.(*kmsg.MetadataResponse)
	expected := ExpectedPartitions(topic)
	if !ok || len(meta.Topics) != 1 || meta.Topics[0].Topic == nil || *meta.Topics[0].Topic != topic || meta.Topics[0].ErrorCode != 0 || len(meta.Topics[0].Partitions) != expected {
		return fmt.Errorf("topic %q unavailable or not %d-partition", topic, expected)
	}
	seen := make([]bool, expected)
	for _, p := range meta.Topics[0].Partitions {
		if p.Partition < 0 || int(p.Partition) >= expected || seen[p.Partition] || p.ErrorCode != 0 || p.Leader < 0 || len(p.ISR) == 0 {
			return fmt.Errorf("topic %q partition unavailable", topic)
		}
		seen[p.Partition] = true
	}
	return nil
}

func ExpectedPartitions(topic string) int {
	if topic == Topic {
		return PartitionCount
	}
	if strings.HasPrefix(topic, "streamtest-v2-") {
		return PartitionCount
	}
	if topic == TopicV1 || strings.HasPrefix(topic, "streamtest-") {
		return 1
	}
	return 0
}
