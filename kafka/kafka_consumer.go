package kafka

import (
	"encoding/json"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/Shopify/sarama"
)

type PartitionMessage struct {
	Consumer sarama.PartitionConsumer
	Msg      *sarama.ConsumerMessage
	BinLog   *global.BinLogData
	Channel  chan *global.BinLogData
}

// KafkaConsumer
type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	c      sarama.Consumer
	closed bool

	partitonsList []int32
	offsetInfo    *OffsetInfo

	partitionConsumerArray []*PartitionMessage
	channel                chan *global.BinLogData
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

	client.partitionConsumerArray = make([]*PartitionMessage, len(client.partitonsList))

	for index, pid := range client.partitonsList {
		offset, ok := client.offsetInfo.PartitionOffset[pid]
		if ok {
			client.partitionConsumerArray[index], err = client.NewPartitionMessgae(pid, offset)
		} else {
			client.partitionConsumerArray[index], err = client.NewPartitionMessgae(pid, sarama.OffsetOldest)
		}
		if err != nil {
			return nil, err
		}
	}

	// 排队咯！
	go func() {
		// Wait for PartitionConsumer to get the Message
		// Usually, all of PartitionConsumers have more than one Message
		length := len(client.partitionConsumerArray)
		for i := 0; i < length; i++ {
			if client.partitionConsumerArray[i].BinLog == nil {
				time.Sleep(time.Second)
			}
			/*
				if client.partitionConsumerArray[i].BinLog == nil {
					// TODO close and panic?
				}
			*/
		}

		for i := 0; i < length-1; i++ {
			tmp := i
			for j := i + 1; j < length; j++ {
				if client.partitionConsumerArray[tmp].BinLog.SeqID > client.partitionConsumerArray[j].BinLog.SeqID {
					tmp = j
				}
			}
			tmpPC := client.partitionConsumerArray[tmp]
			client.partitionConsumerArray[tmp] = client.partitionConsumerArray[i]
			client.partitionConsumerArray[i] = tmpPC
		}

		pc := client.partitionConsumerArray[0]

	}()

	return client, nil
}

func (k *KafkaConsumer) NewPartitionMessgae(pid int32, offset int64) (*PartitionMessage, error) {
	cp, err := k.c.ConsumePartition(k.cfg.Topic, pid, offset)
	if err != nil {
		return nil, err
	}

	pm := &PartitionMessage{
		Consumer: cp,
		BinLog:   nil,
		Msg:      nil,
		Channel:  make(chan *global.BinLogData),
	}
	go func() {
		for msg := range pm.Consumer.Messages() {
			binlog := new(global.BinLogData)
			_ = json.Unmarshal(msg.Value, binlog)
			pm.Msg = msg
			pm.BinLog = binlog
			pm.Channel <- binlog
		}
	}()
	return pm, nil
}

func (k *KafkaConsumer) Message() <-chan *global.BinLogData {
	return k.channel
}

// Close .
func (k *KafkaConsumer) Close() error {
	k.closed = true
	return k.c.Close()
}
