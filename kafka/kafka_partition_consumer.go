package kafka

import (
	"encoding/json"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

// KafkaPartitionConsumer
type KafkaPartitionConsumer struct {
	cfg    KafkaConsumerConfig
	c      sarama.Consumer
	closed bool

	offsetInfo *OffsetInfo

	partitonsList          []int32
	partitionConsumerArray []*PartitionMessage
	channel                []chan *ConsumerMessage

	errChannel chan error
}

// NewKafkaPartitionConsumer .
func NewKafkaPartitionConsumer(config KafkaConsumerConfig) (*KafkaPartitionConsumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.MaxProcessingTime = config.Timeout
	cfg.Consumer.Return.Errors = true
	cfg.ChannelBufferSize = 10
	cfg.Version = sarama.V0_11_0_0

	client := new(KafkaPartitionConsumer)
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

	client.channel = make([]chan *ConsumerMessage, len(client.partitonsList))
	client.errChannel = make(chan error, 1)

	client.partitionConsumerArray = make([]*PartitionMessage, len(client.partitonsList))
	for index, pid := range client.partitonsList {
		client.channel[index] = make(chan *ConsumerMessage)
		offset, ok := client.offsetInfo.PartitionOffset[pid]
		if ok {
			client.partitionConsumerArray[index], err = client.NewPartitionMessgae(pid, offset, index)
		} else {
			client.partitionConsumerArray[index], err = client.NewPartitionMessgae(pid, sarama.OffsetOldest, index)
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
func (k *KafkaPartitionConsumer) NewPartitionMessgae(pid int32, offset int64, index int) (*PartitionMessage, error) {
	cp, err := k.c.ConsumePartition(k.cfg.Topic, pid, offset)
	if err != nil {
		return nil, err
	}

	pm := &PartitionMessage{
		consumer: cp,
	}

	go func() {
		for err := range pm.consumer.Errors() {
			log.Errorf("partitionConsumer error :%s", err)
			k.errChannel <- err
		}
	}()

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

			bMsg := &ConsumerMessage{
				Key:         string(msg.Key),
				Value:       msg.Value,
				PartitionID: msg.Partition,
				Offset:      msg.Offset,
				BinLog:      binlog,
			}
			k.channel[index] <- bMsg
		}
	}()
	return pm, nil
}

func (k *KafkaPartitionConsumer) Message() []chan *ConsumerMessage {
	return k.channel
}

func (k *KafkaPartitionConsumer) Error() <-chan error {
	return k.errChannel
}

func (k *KafkaPartitionConsumer) Callback(cm *ConsumerMessage) {
	log.Debugf("KafkaPartitionConsumer callback seqID %d pid %d offset %d", cm.BinLog.SeqID, cm.PartitionID, cm.Offset)
	k.offsetInfo.Set(cm.BinLog.SeqID, cm.PartitionID, cm.Offset)
	return
}

// Close .
func (k *KafkaPartitionConsumer) Close() {
	if k.closed {
		return
	}
	k.closed = true
	for _, pc := range k.partitionConsumerArray {
		_ = pc.consumer.Close()
	}
	_ = k.c.Close()

	_ = k.offsetInfo.Save()
	log.Info("KafkaPartitionConsumer close...")
	return
}

// Save .
func (k *KafkaPartitionConsumer) Save() {
	_ = k.offsetInfo.Save()
	log.Info("KafkaPartitionConsumer save...")
}

func (k *KafkaPartitionConsumer) Info() []byte {
	if k.offsetInfo != nil {
		return k.offsetInfo.Info()
	}
	return []byte{}
}
