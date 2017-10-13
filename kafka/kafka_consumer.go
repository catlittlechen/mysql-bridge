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
	Msg      *ConsumerMessage
	Channel  chan *ConsumerMessage
}

// KafkaConsumer
type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	c      sarama.Consumer
	closed bool

	offsetInfo    *OffsetInfo
	partitonsList []int32

	partitionConsumerArray []*PartitionMessage
	channel                chan *ConsumerMessage
	seqID                  uint64
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

	// 排队咯！
	go func() {
		// Wait for PartitionConsumer to get the Message
		// Usually, all of PartitionConsumers have more than one Message
		length := len(client.partitionConsumerArray)
		for i := 0; i < length; i++ {
			if client.partitionConsumerArray[i].Msg == nil {
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
				if client.partitionConsumerArray[tmp].Msg.BinLog.SeqID > client.partitionConsumerArray[j].Msg.BinLog.SeqID {
					tmp = j
				}
			}
			tmpPC := client.partitionConsumerArray[tmp]
			client.partitionConsumerArray[tmp] = client.partitionConsumerArray[i]
			client.partitionConsumerArray[i] = tmpPC
		}

		pc := client.partitionConsumerArray[0]
		if client.offsetInfo.SequenceID == 0 {
			client.seqID = pc.Msg.BinLog.SeqID
		} else {
			client.seqID = client.offsetInfo.SequenceID
		}

		for {
			if client.closed {
				break
			}

			pc = client.partitionConsumerArray[0]

			// less bug?
			if client.seqID > pc.Msg.BinLog.SeqID {
				// TODO
			}

			pc.Msg = nil
			msg := <-pc.Channel

			client.channel <- msg
			client.seqID += 1

			if client.seqID > global.MaxSeqID {
				client.seqID = global.MinSeqID
			}

			for i := 0; i < 3; i++ {
				if pc.Msg == nil {
					time.Sleep(time.Second)
				}
			}
			/*
				if pc.Msg == nil {
					// TODO
				}
			*/

			for i := 0; i < length-1; i++ {
				if client.partitionConsumerArray[i].Msg.BinLog.SeqID < client.partitionConsumerArray[i+1].Msg.BinLog.SeqID {
					break
				}

				tmpPC := client.partitionConsumerArray[i]
				client.partitionConsumerArray[i] = client.partitionConsumerArray[i+1]
				client.partitionConsumerArray[i+1] = tmpPC
			}
		}

	}()

	return client, nil
}

func (k *KafkaConsumer) NewPartitionMessgae(pid int32, offset int64) (*PartitionMessage, error) {
	cp, err := k.c.ConsumePartition(k.cfg.Topic, pid, offset)
	if err != nil {
		return nil, err
	}

	pm := &PartitionMessage{
		consumer: cp,
		Msg:      nil,
		Channel:  make(chan *ConsumerMessage),
	}
	go func() {
		for msg := range pm.consumer.Messages() {
			binlog := new(global.BinLogData)
			_ = json.Unmarshal(msg.Value, binlog)
			pm.Msg = &ConsumerMessage{
				Msg:    msg,
				BinLog: binlog,
			}
			pm.Channel <- pm.Msg
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
