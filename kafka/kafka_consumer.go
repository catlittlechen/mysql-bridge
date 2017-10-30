package kafka

import (
	"encoding/json"
	"strings"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type ConsumerMessage struct {
	PartitionID int32
	Offset      int64
	BinLog      *global.BinLogData
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

	ring *RingBuffer

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
	log.Infof("kafka client loadOffsetInfo success")

	client.partitonsList, err = client.c.Partitions(config.Topic)
	if err != nil {
		return nil, err
	}
	log.Infof("kafka client get partitions list %+v success", client.partitonsList)

	seqID := config.DefaultSeqID
	if seqID == 0 {
		seqID = client.offsetInfo.SequenceID + 1
		if seqID > global.MaxSeqID {
			seqID = global.MinSeqID
		}
	}
	client.ring = NewRingBuffer(config.RingLen, seqID, config.TimeSleep)
	client.channel = client.ring.channel

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
		log.Infof("kafka client get %d partitions success", pid)
	}
	log.Infof("kafka client get all partitions success")

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
			if k.closed {
				break
			}

			if msg == nil {
				// TODO alterover?
				log.Errorf("partitionConsumer get msg is nil")
				break
			}

			binlog := new(global.BinLogData)
			_ = json.Unmarshal(msg.Value, binlog)

			keys := strings.Split(string(msg.Key), "-")
			if len(keys) != 5 {
				log.Warnf("fix bug for msg key seqID:%d pid:%d offset:%d", binlog.SeqID, msg.Partition, msg.Offset)
				continue
			}

			bMsg := &ConsumerMessage{
				PartitionID: msg.Partition,
				Offset:      msg.Offset,
				BinLog:      binlog,
			}
			k.ring.Set(bMsg)
		}
	}()
	return pm, nil
}

func (k *KafkaConsumer) Message() <-chan *ConsumerMessage {
	return k.channel
}

func (k *KafkaConsumer) Callback(cm *ConsumerMessage) {
	log.Infof("KafkaConsumer callback seqID %d pid %d offset %d", cm.BinLog.SeqID, cm.PartitionID, cm.Offset)
	k.offsetInfo.Set(cm.BinLog.SeqID, cm.PartitionID, cm.Offset)
	return
}

// Close .
func (k *KafkaConsumer) Close() {
	if k.closed {
		return
	}
	k.closed = true
	for _, pc := range k.partitionConsumerArray {
		_ = pc.consumer.Close()
	}
	_ = k.c.Close()

	k.ring.Close()
	_ = k.offsetInfo.Save()
	log.Info("KafkaConsumer close...")
	return
}

// Save .
func (k *KafkaConsumer) Save() {
	_ = k.offsetInfo.Save()
	log.Info("KafkaConsumer save...")
}

func (k *KafkaConsumer) Info() []byte {
	if k.offsetInfo != nil {
		return k.offsetInfo.Info()
	}
	return nil
}
