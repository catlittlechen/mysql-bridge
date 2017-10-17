// Author: chenkai@youmi.net
package kafka

import (
	"time"
)

// KafkaProducerConfig .
type KafkaProducerConfig struct {
	BrokerList []string      `yaml:"broker_list"`
	FlushTime  time.Duration `yaml:"flush_time"`
}

// KafkaConsumerConfig .
type KafkaConsumerConfig struct {
	BrokerList []string      `yaml:"broker_list"`
	Timeout    time.Duration `yaml:"timeout"`
	Topic      string        `yaml:"topic"`

	OffsetDir    string        `yaml:"offset_dir"`
	DefaultSeqID uint64        `yaml:"default_seq_id"`
	Ticker       time.Duration `yaml:"ticker"`
	RingLen      int           `yaml:"ring_len"`
	TimeSleep    time.Duration `yaml:"time_sleep"`
}
