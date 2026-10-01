package stream

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CheckTopic checks the declared single-partition topic without creating it.
func CheckTopic(ctx context.Context, client *kgo.Client, topic string) error {
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{{Topic: kmsg.StringPtr(topic)}}
	req.AllowAutoTopicCreation = false
	response, err := client.Request(ctx, req)
	if err != nil {
		return err
	}
	meta, ok := response.(*kmsg.MetadataResponse)
	if !ok || len(meta.Topics) != 1 || meta.Topics[0].Topic == nil || *meta.Topics[0].Topic != topic || meta.Topics[0].ErrorCode != 0 || len(meta.Topics[0].Partitions) != 1 {
		return fmt.Errorf("topic %q unavailable or not single-partition", topic)
	}
	p := meta.Topics[0].Partitions[0]
	if p.Partition != 0 || p.ErrorCode != 0 || p.Leader < 0 || len(p.ISR) == 0 {
		return fmt.Errorf("topic %q partition unavailable", topic)
	}
	return nil
}
