// Author: chenkai@youmi.net

package main

import (
	"encoding/json"

	"git.umlife.net/backend/mysql-bridge/kafka"
	"git.umlife.net/backend/mysql-bridge/tcp"
)

type Sink struct {
	kproducer *kafka.KafkaProducer
	tSink     *tcp.Sink
	btSink    []*tcp.Sink
}

func NewSink() (*Sink, error) {
	client := new(Sink)
	var err error

	switch channelCfg.SinkType {
	case TCPType:
		// Sink 一开始就没必要可以连接,所以忽略错误
		client.tSink, _ = tcp.NewSink(channelCfg.Sink)
		for _, cfg := range channelCfg.BroadcastSink {
			obj, _ := tcp.NewSink(cfg)
			client.btSink = append(client.btSink, obj)
		}
	case KafkaType:
		client.kproducer, err = kafka.NewKafkaProducer(channelCfg.KafkaProducer)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func (sink *Sink) Sink(bMsg []byte) error {
	var err error
	switch channelCfg.SinkType {
	case TCPType:
		err = sink.tSink.Write(bMsg)
		if err == nil {
			return nil
		}
		if len(sink.btSink) == 0 {
			return err
		}

		channel := make(chan bool, 1)
		okChannel := make(chan bool, 1)
		for _, s := range sink.btSink {
			go func() {
				serr := s.Write(bMsg)
				channel <- (serr == nil)
			}()
		}

		go func() {
			for i := 0; i < len(sink.btSink); i++ {
				result := <-channel
				if result {
					okChannel <- true
				}
			}
			close(okChannel)
		}()
		result := <-okChannel
		if result {
			return nil
		}
		return err

	case KafkaType:
		obj := new(Message)
		err = json.Unmarshal(bMsg, obj)
		if err != nil {
			return err
		}

		for _, kv := range obj.Data {
			err = sink.kproducer.SendWithKey(obj.Topic, kv.Key, kv.Value)
			if err != nil {
				return err
			}
		}

		return nil
	}

	return nil
}
