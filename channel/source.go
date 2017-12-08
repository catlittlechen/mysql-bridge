// Author: chenkai@youmi.net

package main

import (
	"encoding/json"
	"errors"
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
	kconsumer *kafka.KafkaPartitionConsumer
	sink      SinkAdapter
}

func (k *KafkaSourceAdapter) New(sink SinkAdapter) error {
	k.sink = sink
	var err error
	k.kconsumer, err = kafka.NewKafkaPartitionConsumer(channelCfg.KafkaConsumer)
	if err != nil {
		return err
	}
	go func() {
		for kerr := range k.kconsumer.Error() {
			log.Errorf("kconsumer error :%s", kerr)
		}
	}()

	err = k.Consumer(nil)
	if err != nil {
		log.Errorf("kconsumer consumer failed. err:%s", err)
	}
	return err
}

func (k *KafkaSourceAdapter) Consumer(data []byte) error {
	log.Debugf("KafkaSourceAdapter Consumer data:%s", data)

	kcmChannel := k.kconsumer.Message()
	if kcmChannel == nil {
		return errors.New("kafka consumer get message channel is nil")
	}
	for index := range kcmChannel {
		go func(id int) {
			log.Infof("KafkaSourceAdapter get channel. index:%d", id)
			kerr := k.consumer(kcmChannel[id])
			if kerr != nil {
				log.Errorf("kconsumer consumer failed. err:%s", kerr)
			}
			return
		}(index)
	}
	return nil
}

func (k *KafkaSourceAdapter) consumer(channel chan *kafka.ConsumerMessage) error {
	ticker := time.Tick(channelCfg.KafkaConsumerExt.MaxIdelTime)
	var msg *kafka.ConsumerMessage

	topic := channelCfg.KafkaConsumer.Topic
	if channelCfg.KafkaConsumerExt.DefaultTopic != "" {
		topic = channelCfg.KafkaConsumerExt.DefaultTopic
	}
	message := &Message{
		Topic: topic,
	}
	length := 0
	count := 0
	clean := false
	for {
		select {
		case <-ticker:
			log.Debugf("ticker!")
			if count != 0 {
				clean = true
			}
		case msg = <-channel:
			log.Debugf("kcmChannel get msg: %+v", msg)
			if msg == nil {
				log.Warnf("kcmChannel get msg is nil")
				return errors.New("kafka channel get nil msg")
			}
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

			data, err = ZlibEncode(data)
			if err != nil {
				log.Errorf("zlibencode failed. err:%s", err)
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
				Topic: topic,
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
	log.Debugf("TCPSourceAdapter Consumer data:%s", data)
	return t.sink.Produce(data)
}

func (t *TCPSourceAdapter) Close() error {
	return nil
}
