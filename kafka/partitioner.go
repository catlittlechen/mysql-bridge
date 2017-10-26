// Author: catlittlechen@gmail.com

package kafka

import "github.com/Shopify/sarama"

type DefaultPartitioner struct {
}

func NewDefaultPartitioner(topic string) sarama.Partitioner {
	p := new(DefaultPartitioner)
	return p
}

// Partition takes a message and partition count and chooses a partition
func (p *DefaultPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	if message.Key == nil {
		return 0, nil
	}
	bytes, err := message.Key.Encode()
	if err != nil {
		return -1, err
	}
	partition := int32(MurmurHash2(bytes) % uint32(numPartitions))
	return partition, nil
}

func (p *DefaultPartitioner) RequiresConsistency() bool {
	return true
}
