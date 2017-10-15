package kafka

import (
	"encoding/json"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/Shopify/sarama"
)

type ConsumerMessage struct {
	Msg    *sarama.ConsumerMessage
	BinLog *global.BinLogData
}

type PartitionMessage struct {
	consumer sarama.PartitionConsumer
}

// KafkaConsumer
type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	c      sarama.Consumer
	closed bool

	offsetInfo *OffsetInfo
	batter     uint64

	partitonsList []int32

	ring []*ConsumerMessage
	now  int

	partitionConsumerArray []*PartitionMessage
	channel                chan *ConsumerMessage
}

// NewKafkaConsumer .
func NewKafkaConsumer(config KafkaConsumerConfig) (*KafkaConsumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.MaxProcessingTime = config.Timeout

	client := new(KafkaConsumer)
	client.cfg = config

	var err error
	client.c, err = sarama.NewConsumer(config.BrokerList, cfg)
	if err != nil {
		return nil, err
	}
	client.closed = false

	client.offsetInfo, err = loadOffsetInfo(config.OffsetDir, config.Ticker)
	if err != nil {
		return nil, err
	}

	client.partitonsList, err = client.c.Partitions(config.Topic)
	if err != nil {
		return nil, err
	}

	client.ring = make([]*ConsumerMessage, config.RingLen)
	client.now = 0
	if client.offsetInfo.SequenceID == 0 {
		client.batter = config.DefaultSeqID
	} else {
		client.batter = client.offsetInfo.SequenceID
	}

	client.partitionConsumerArray = make([]*PartitionMessage, len(client.partitonsList))
	client.channel = make(chan *ConsumerMessage, 1024)

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

	go func() {
		for {
			client.now += 1
			if client.now == client.cfg.RingLen {
				client.now = 0
			}
			for client.ring[client.now] == nil {
				time.Sleep(time.Second)
				continue
			}
			client.channel <- client.ring[client.now]
			client.ring[client.now] = nil
			if client.now == 0 {
				client.batter += uint64(client.cfg.RingLen)
			}
		}
	}()

	return client, nil
}

// batter/0    batter+now-1/now-1  batter-len+now/now      batter-1/len-1
func (k *KafkaConsumer) NewPartitionMessgae(pid int32, offset int64) (*PartitionMessage, error) {
	cp, err := k.c.ConsumePartition(k.cfg.Topic, pid, offset)
	if err != nil {
		return nil, err
	}

	pm := &PartitionMessage{
		consumer: cp,
	}
	go func() {
		for msg := range pm.consumer.Messages() {
			binlog := new(global.BinLogData)
			_ = json.Unmarshal(msg.Value, binlog)
			for {
				if k.batter-uint64(k.cfg.RingLen-k.now) > binlog.SeqID {
					break
				}

				if k.batter-uint64(k.cfg.RingLen-k.now) <= binlog.SeqID && binlog.SeqID < k.batter+uint64(k.now)-1 {
					if k.batter > binlog.SeqID {
						k.ring[k.cfg.RingLen-int(k.batter-binlog.SeqID)] = &ConsumerMessage{
							Msg:    msg,
							BinLog: binlog,
						}
					} else {
						k.ring[binlog.SeqID-k.batter] = &ConsumerMessage{
							Msg:    msg,
							BinLog: binlog,
						}
					}
					break
				}

				time.Sleep(time.Second)
			}
		}
	}()
	return pm, nil
}

func (k *KafkaConsumer) Message() <-chan *ConsumerMessage {
	return k.channel
}

// TODO
func (k *KafkaConsumer) Callback(cm *ConsumerMessage) {
	k.offsetInfo.SequenceID = cm.BinLog.SeqID
	k.offsetInfo.PartitionOffset[cm.Msg.Partition] = cm.Msg.Offset
	return
}

// Close .
func (k *KafkaConsumer) Close() {
	k.closed = true
	for _, pc := range k.partitionConsumerArray {
		_ = pc.consumer.Close()
	}
	_ = k.c.Close()
	return
}
