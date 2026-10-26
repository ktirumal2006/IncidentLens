package streamstatus

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaProbe uses a separate client and only metadata/offset read requests.
type KafkaProbe struct {
	client *kgo.Client
	topic  string
	group  string
}

func NewKafkaProbe(brokers []string, topic, group string) (*KafkaProbe, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, errors.New("status probe configuration incomplete")
	}
	// franz-go validates the fetch budget even for a read-only admin client.
	// Bound both consistently; this client never subscribes to a topic.
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.FetchMaxBytes(1<<20), kgo.BrokerMaxReadBytes(1<<20))
	if err != nil {
		return nil, err
	}
	return &KafkaProbe{client: client, topic: topic, group: group}, nil
}

func (p *KafkaProbe) Close() { p.client.Close() }

func (p *KafkaProbe) Observe(ctx context.Context) Observation {
	if p == nil || p.client == nil {
		return Observation{ReasonCode: "metadata_unavailable"}
	}
	meta := kmsg.NewPtrMetadataRequest()
	meta.AllowAutoTopicCreation = false
	meta.Topics = []kmsg.MetadataRequestTopic{{Topic: &p.topic}}
	metadata, err := meta.RequestWith(ctx, p.client)
	if err != nil || len(metadata.Topics) != 1 || metadata.Topics[0].ErrorCode != 0 {
		return Observation{ReasonCode: "metadata_unavailable"}
	}
	if metadata.Topics[0].Topic == nil || *metadata.Topics[0].Topic != p.topic {
		return Observation{ReasonCode: "metadata_unavailable"}
	}
	if len(metadata.Topics[0].Partitions) != ExpectedPartitions {
		return Observation{ReasonCode: "partition_coverage_incomplete"}
	}
	seen := [ExpectedPartitions]bool{}
	for _, partition := range metadata.Topics[0].Partitions {
		id := partition.Partition
		if id < 0 || id >= ExpectedPartitions || seen[id] || partition.ErrorCode != 0 {
			return Observation{ReasonCode: "partition_coverage_inconsistent"}
		}
		seen[id] = true
	}
	starts, reason := p.offsets(ctx, -2)
	if reason != "" {
		return Observation{ReasonCode: reason}
	}
	// Read commits before end offsets: an active consumer may commit a record
	// while the snapshot is sampled, and its commit must not outrun our end read.
	committed, reason := p.committed(ctx, metadata.Topics[0].TopicID)
	if reason != "" {
		return Observation{ReasonCode: reason}
	}
	ends, reason := p.offsets(ctx, -1)
	if reason != "" {
		return Observation{ReasonCode: reason}
	}
	parts := make([]Partition, 0, ExpectedPartitions)
	for id := int32(0); id < ExpectedPartitions; id++ {
		parts = append(parts, Partition{ID: id, LogStart: starts[id], End: ends[id], Committed: committed[id]})
	}
	return Observation{Partitions: parts}
}

func (p *KafkaProbe) offsets(ctx context.Context, timestamp int64) ([ExpectedPartitions]int64, string) {
	var values [ExpectedPartitions]int64
	req := kmsg.NewPtrListOffsetsRequest()
	partitions := make([]kmsg.ListOffsetsRequestTopicPartition, ExpectedPartitions)
	for id := range partitions {
		partitions[id] = kmsg.NewListOffsetsRequestTopicPartition()
		partitions[id].Partition = int32(id)
		partitions[id].Timestamp = timestamp
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{{Topic: p.topic, Partitions: partitions}}
	response, err := req.RequestWith(ctx, p.client)
	if err != nil || len(response.Topics) != 1 || response.Topics[0].Topic != p.topic || len(response.Topics[0].Partitions) != ExpectedPartitions {
		return values, "offsets_unavailable"
	}
	seen := [ExpectedPartitions]bool{}
	for _, partition := range response.Topics[0].Partitions {
		id := partition.Partition
		if id < 0 || id >= ExpectedPartitions || seen[id] || partition.ErrorCode != 0 {
			return values, "offsets_unavailable"
		}
		seen[id] = true
		values[id] = partition.Offset
	}
	return values, ""
}

func (p *KafkaProbe) committed(ctx context.Context, topicID [16]byte) ([ExpectedPartitions]int64, string) {
	var values [ExpectedPartitions]int64
	req := kmsg.NewPtrOffsetFetchRequest()
	partitions := []int32{0, 1, 2, 3}
	req.Group = p.group
	req.Topics = []kmsg.OffsetFetchRequestTopic{{Topic: p.topic, Partitions: partitions}}
	req.Groups = []kmsg.OffsetFetchRequestGroup{{Group: p.group, Topics: []kmsg.OffsetFetchRequestGroupTopic{{Topic: p.topic, Partitions: partitions}}}}
	response, err := req.RequestWith(ctx, p.client)
	if err != nil {
		return values, "offsets_unavailable"
	}
	var got []kmsg.OffsetFetchResponseGroupTopicPartition
	if len(response.Groups) == 1 {
		group := response.Groups[0]
		if group.ErrorCode != 0 || len(group.Topics) != 1 || !sameTopic(group.Topics[0].Topic, group.Topics[0].TopicID, p.topic, topicID) {
			return values, "offsets_unavailable"
		}
		got = group.Topics[0].Partitions
	} else {
		if response.ErrorCode != 0 || len(response.Topics) != 1 || response.Topics[0].Topic != p.topic {
			return values, "offsets_unavailable"
		}
		for _, partition := range response.Topics[0].Partitions {
			got = append(got, kmsg.OffsetFetchResponseGroupTopicPartition{Partition: partition.Partition, Offset: partition.Offset, ErrorCode: partition.ErrorCode})
		}
	}
	if len(got) != ExpectedPartitions {
		return values, "partition_coverage_incomplete"
	}
	seen := [ExpectedPartitions]bool{}
	for _, partition := range got {
		id := partition.Partition
		if id < 0 || id >= ExpectedPartitions || seen[id] || partition.ErrorCode != 0 {
			return values, "partition_coverage_inconsistent"
		}
		seen[id] = true
		values[id] = partition.Offset
	}
	return values, ""
}

func sameTopic(name string, id [16]byte, expectedName string, expectedID [16]byte) bool {
	if id != ([16]byte{}) && expectedID != ([16]byte{}) {
		return id == expectedID
	}
	return name == expectedName
}
