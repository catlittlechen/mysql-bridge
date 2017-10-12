package kafka

import (
	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/Shopify/sarama"
)

type PartitionMessage struct {
	Consumer sarama.PartitionConsumer
	Msg      *sarama.ConsumerMessage
	BinLog   *global.BinLogData
}

// KafkaConsumer
type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	c      sarama.Consumer
	closed bool

	partitonsList []int32
	offsetInfo    *OffsetInfo

	messageChannelMap    map[int32]chan *global.BinLogData
	partitionConsumerMap map[int32]sarama.PartitionConsumer
	partitionQueue       []*PartitionMessage

	channel chan *global.BinLogData
}

// NewKafkaConsumer .
func NewKafkaConsumer(config KafkaConsumerConfig) (*KafkaConsumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.MaxProcessingTime = config.Timeout

	client := new(KafkaConsumer)
	var err error
	client.c, err = sarama.NewConsumer(config.BrokerList, cfg)
	if err != nil {
		return nil, err
	}
	client.cfg = config
	client.closed = false

	client.offsetInfo, err = loadOffsetInfo(config.OffsetDir, config.Ticker)
	if err != nil {
		return nil, err
	}

	client.partitonsList, err = client.c.Partitions(config.Topic)
	if err != nil {
		return nil, err
	}

	client.messageChannelMap = make(map[int32]chan *global.BinLogData)
	client.partitionConsumerMap = make(map[int32]sarama.PartitionConsumer)
	client.channel = make(chan *global.BinLogData)

	for _, pid := range client.partitonsList {
		client.messageChannelMap[pid] = make(chan *global.BinLogData)
		offset, ok := client.offsetInfo.PartitionOffset[pid]
		if ok {
			client.partitionConsumerMap[pid], err = client.c.ConsumePartition(config.Topic, pid, offset)
		} else {
			client.partitionConsumerMap[pid], err = client.c.ConsumePartition(config.Topic, pid, sarama.OffsetOldest)
		}
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

// Close .
func (k *KafkaConsumer) Close() error {
	k.closed = true
	return k.c.Close()
}
