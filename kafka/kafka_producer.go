package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/satori/go.uuid"
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
	cfg.Producer.Compression = sarama.CompressionSnappy
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

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
		Key:   sarama.ByteEncoder(uuid.NewV4().Bytes()),
		Value: sarama.ByteEncoder(data),
	}
	_, _, err = k.p.SendMessage(msg)
	return
}

// Close .
func (k *KafkaProducer) Close() error {
	k.closed = true
	return k.p.Close()
}