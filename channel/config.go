// Author: chenkai@youmi.net

package main

import (
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"git.umlife.net/backend/mysql-bridge/kafka"
	"git.umlife.net/backend/mysql-bridge/tcp"
)

var channelCfg Config

const (
	TCPType   = 1
	KafkaType = 2
)

type Config struct {
	Logconf string `yaml:"log_config"`

	SourceType       int                       `yaml:"source_type"`
	Source           tcp.SourceConfig          `yaml:"source_tcp"`
	KafkaConsumer    kafka.KafkaConsumerConfig `yaml:"source_kafka"`
	KafkaConsumerExt KafkaConsumerExtConfig    `yaml:"source_kafka_ext"`

	SinkType      int                       `yaml:"sink_type"`
	Sink          tcp.SinkConfig            `yaml:"sink_tcp"`
	BroadcastSink []tcp.SinkConfig          `yaml:"broadcast_sink_tcp"`
	KafkaProducer kafka.KafkaProducerConfig `yaml:"sink_kafka"`
}

type KafkaConsumerExtConfig struct {
	MaxCount     int           `yaml:"max_count"`
	MaxSize      int           `yaml:"max_size"`
	MaxIdelTime  time.Duration `yaml:"max_idel_time"`
	FailSleep    time.Duration `yaml:"fail_sleep"`
	DefaultTopic string        `yaml:"default_topic"`
}

func ParseConfigFile(filepath string) error {
	err := global.ParseYamlFile(filepath, &channelCfg)
	if err != nil {
		return err
	}

	return nil
}
