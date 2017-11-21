// Author: chenkai@youmi.net

package main

import (
	"encoding/json"

	"git.umlife.net/backend/mysql-bridge/kafka"
	"git.umlife.net/backend/mysql-bridge/tcp"
)

type SinkAdapter interface {
	New() error
	Produce([]byte) error
}

type KafkaSinkAdapter struct {
	kproducer *kafka.KafkaProducer
}

func (k *KafkaSinkAdapter) New() error {
	var err error
	k.kproducer, err = kafka.NewKafkaProducer(channelCfg.KafkaProducer)
	return err
}

func (k *KafkaSinkAdapter) Produce(bMsg []byte) error {
	obj := new(Message)
	err := json.Unmarshal(bMsg, obj)
	if err != nil {
		return err
	}

	for _, kv := range obj.Data {
		err = k.kproducer.SendWithKey(obj.Topic, kv.Key, kv.Value)
		if err != nil {
			return err
		}
	}

	return nil
}

type TCPSinkAdapter struct {
	tSink  *tcp.Sink
	btSink []*tcp.Sink
}

func (t *TCPSinkAdapter) New() error {
	// Sink 一开始就没必要可以连接,所以忽略错误
	t.tSink, _ = tcp.NewSink(channelCfg.Sink)
	for _, cfg := range channelCfg.BroadcastSink {
		obj, _ := tcp.NewSink(cfg)
		t.btSink = append(t.btSink, obj)
	}
	return nil
}

func (t *TCPSinkAdapter) Produce(bMsg []byte) error {
	err := t.tSink.Write(bMsg)
	if err == nil {
		return nil
	}
	if len(t.btSink) == 0 {
		return err
	}

	channel := make(chan bool, 1)
	okChannel := make(chan bool, 1)
	for _, s := range t.btSink {
		go func() {
			serr := s.Write(bMsg)
			channel <- (serr == nil)
		}()
	}

	go func() {
		for i := 0; i < len(t.btSink); i++ {
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
}
