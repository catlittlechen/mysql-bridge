// Author: chenkai@youmi.net

package main

import (
	"encoding/json"
	"time"

	"git.umlife.net/backend/mysql-bridge/kafka"
	"git.umlife.net/backend/mysql-bridge/tcp"
	log "github.com/sirupsen/logrus"
)

type SourceAdapter interface {
	New(sink SinkAdapter) error
	Consumer([]byte) error
	Close() error
}

type KafkaSourceAdapter struct {
	kconsumer *kafka.KafkaConsumer
	sink      SinkAdapter
}

func (k *KafkaSourceAdapter) New(sink SinkAdapter) error {
	k.sink = sink
	var err error
	k.kconsumer, err = kafka.NewKafkaConsumer(channelCfg.KafkaConsumer)
	if err != nil {
		return err
	}
	go func() {
		for kerr := range k.kconsumer.Error() {
			log.Errorf("kconsumer error :%s", kerr)
		}
	}()
	return nil
	// do something
}

func (k *KafkaSourceAdapter) Consumer(data []byte) error {

	ticker := time.Tick(channelCfg.KafkaConsumerExt.MaxIdelTime)
	kcmChannel := k.kconsumer.Message()
	var msg *kafka.ConsumerMessage

	message := &Message{
		Topic: channelCfg.KafkaConsumer.Topic,
	}
	length := 0
	count := 0
	clean := false
	for {
		select {
		case <-ticker:
			clean = true
		case msg = <-kcmChannel:
			message.Data = append(message.Data, KV{
				Key:   msg.Key,
				Value: msg.Value,
			})
			length += len(msg.Value)
			count++
			if length > channelCfg.KafkaConsumerExt.MaxSize || count > channelCfg.KafkaConsumerExt.MaxCount {
				clean = true
			}
		}
		if clean {
			data, err := json.Marshal(message)
			if err != nil {
				log.Errorf("json Marshal failed. err:%s", err)
				return err
			}

			// kafka是源头的话，error直接一直重试吧。
			for {
				err = k.sink.Produce(data)
				if err == nil {
					break
				}
				log.Errorf("kconsumer sink produce failed. err:%s", err)
				time.Sleep(channelCfg.KafkaConsumerExt.FailSleep)
			}
			k.kconsumer.Callback(msg)
			message = &Message{
				Topic: channelCfg.KafkaConsumer.Topic,
			}
			length = 0
			count = 0
			clean = false
		}
	}
	return nil
}

func (k *KafkaSourceAdapter) Close() error {
	if k.kconsumer == nil {
		return nil
	}
	k.kconsumer.Close()
	return nil
}

type TCPSourceAdapter struct {
	tsource *tcp.Source
	sink    SinkAdapter
}

func (t *TCPSourceAdapter) New(sink SinkAdapter) error {
	t.sink = sink
	var err error
	t.tsource, err = tcp.NewSource(channelCfg.Source, t.Consumer)
	if err != nil {
		return err
	}
	go func() {
		for terr := range t.tsource.Error() {
			log.Errorf("tsource error :%s", terr)
		}
	}()

	return nil
}

func (t *TCPSourceAdapter) Consumer(data []byte) error {
	return t.sink.Produce(data)
}

func (t *TCPSourceAdapter) Close() error {
	return nil
}