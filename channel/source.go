// Author: chenkai@youmi.net

package main

import (
	"git.umlife.net/backend/mysql-bridge/kafka"
	"git.umlife.net/backend/mysql-bridge/tcp"
)

type Source struct {
	kconsumer *kafka.KafkaConsumer
	tsource   *tcp.Source

	sink *Sink
}

func NewSource(sink *Sink) (*Source, error) {
	client := new(Source)
	client.sink = sink

	var err error
	switch channelCfg.SourceType {
	case TCPType:
		client.tsource, err = tcp.NewSource(channelCfg.Source, nil)
		if err != nil {
			return nil, err
		}
	case KafkaType:
		client.kconsumer, err = kafka.NewKafkaConsumer(channelCfg.KafkaConsumer)
		if err != nil {
			return nil, err
		}
		// do something
	}
	return client, nil
}
