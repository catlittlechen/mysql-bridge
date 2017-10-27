package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

// KafkaProducer .
type KafkaProducer struct {
	cfg    KafkaProducerConfig
	p      sarama.SyncProducer
	closed bool
}

// NewKafkaProducer .
func NewKafkaProducer(config KafkaProducerConfig) (*KafkaProducer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Compression = sarama.CompressionGZIP
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Producer.Partitioner = NewDefaultPartitioner

	var err error
	client := new(KafkaProducer)
	client.p, err = sarama.NewSyncProducer(config.BrokerList, cfg)
	if err != nil {
		return nil, err
	}
	client.cfg = config
	client.closed = false

	return client, nil
}

// Send 发送信息到指定的topic
func (k *KafkaProducer) Send(topic string, data []byte) (err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(uuid.NewV4().String()),
		Value: sarama.ByteEncoder(data),
	}
	_, _, err = k.p.SendMessage(msg)
	return
}

// Close .
func (k *KafkaProducer) Close() error {
	k.closed = true
	err := k.p.Close()
	if err != nil {
		return err
	}
	log.Info("KafkaProducer close success")
	return nil
}
