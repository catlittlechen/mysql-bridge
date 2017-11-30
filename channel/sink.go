// Author: chenkai@youmi.net

package main

import (
	"encoding/json"

	"git.umlife.net/backend/mysql-bridge/kafka"
	"git.umlife.net/backend/mysql-bridge/tcp"
	log "github.com/sirupsen/logrus"
)

type SinkAdapter interface {
	New() error
	Produce([]byte) error
	Close() error
}

type LogSinkAdapter struct {
}

func (l *LogSinkAdapter) New() error {
	return nil
}

func (l *LogSinkAdapter) Produce(bMsg []byte) error {
	log.Info(string(bMsg))
	return nil
}

func (l *LogSinkAdapter) Close() error {
	return nil
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
	log.Debugf("KafkaSinkAdapter Produce data:%s", bMsg)
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

func (k *KafkaSinkAdapter) Close() error {
	if k.kproducer == nil {
		return nil
	}
	return k.kproducer.Close()
}

type TCPSinkAdapter struct {
	tSink  *tcp.Sink
	btSink []*tcp.Sink
}

func (t *TCPSinkAdapter) New() error {
	var err error
	t.tSink, err = tcp.NewSink(channelCfg.Sink)
	if err != nil {
		return err
	}
	for _, cfg := range channelCfg.BroadcastSink {
		obj, err := tcp.NewSink(cfg)
		if err != nil {
			return err
		}
		t.btSink = append(t.btSink, obj)
	}
	return nil
}

func (t *TCPSinkAdapter) Produce(bMsg []byte) error {
	log.Debugf("TCPSinkAdapter Produce data:%s", bMsg)
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
			if serr != nil {
				log.Errorf("BroadcastSink error:%s", serr)
				channel <- false
			} else {
				channel <- true
			}
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

func (t *TCPSinkAdapter) Close() error {
	return nil
}
