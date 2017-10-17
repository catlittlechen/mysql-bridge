package kafka

import (
	"encoding/json"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type ConsumerMessage struct {
	Msg    *sarama.ConsumerMessage
	BinLog *global.BinLogData
}

type PartitionMessage struct {
	consumer sarama.PartitionConsumer
}

// KafkaConsumer
type KafkaConsumer struct {
	cfg    KafkaConsumerConfig
	c      sarama.Consumer
	closed bool

	offsetInfo *OffsetInfo
	batter     uint64

	partitonsList []int32

	ring []*ConsumerMessage
	now  int

	partitionConsumerArray []*PartitionMessage
	channel                chan *ConsumerMessage
}

// NewKafkaConsumer .
func NewKafkaConsumer(config KafkaConsumerConfig) (*KafkaConsumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.MaxProcessingTime = config.Timeout

	client := new(KafkaConsumer)
	client.cfg = config

	var err error
	client.c, err = sarama.NewConsumer(config.BrokerList, cfg)
	if err != nil {
		return nil, err
	}
	client.closed = false

	client.offsetInfo, err = loadOffsetInfo(config.OffsetDir, config.Ticker)
	if err != nil {
		return nil, err
	}
	log.Infof("kafka client loadOffsetInfo success")

	client.partitonsList, err = client.c.Partitions(config.Topic)
	if err != nil {
		return nil, err
	}
	log.Infof("kafka client get partitions list %+v success", client.partitonsList)

	client.ring = make([]*ConsumerMessage, config.RingLen)
	client.now = 1
	if client.offsetInfo.SequenceID == 0 {
		client.batter = (config.DefaultSeqID + uint64(config.RingLen)) % global.MaxSeqID
	} else {
		client.batter = (client.offsetInfo.SequenceID + uint64(config.RingLen)) % global.MaxSeqID
	}

	client.partitionConsumerArray = make([]*PartitionMessage, len(client.partitonsList))
	client.channel = make(chan *ConsumerMessage, 1024)

	for index, pid := range client.partitonsList {
		offset, ok := client.offsetInfo.PartitionOffset[pid]
		if ok {
			client.partitionConsumerArray[index], err = client.NewPartitionMessgae(pid, offset)
		} else {
			client.partitionConsumerArray[index], err = client.NewPartitionMessgae(pid, sarama.OffsetOldest)
		}
		if err != nil {
			return nil, err
		}
		log.Infof("kafka client get %d partitions success", pid)
	}
	log.Infof("kafka client get all partitions success")

	go func() {
		exceptSeqID := client.batter - uint64(config.RingLen) + 1
		if client.batter < uint64(config.RingLen) {
			exceptSeqID = client.batter + global.MaxSeqID - uint64(config.RingLen) + 1
		}
		for {
			for {
				if client.closed {
					return
				}
				if client.ring[client.now] != nil {
					if client.ring[client.now].BinLog.SeqID == exceptSeqID {
						log.Infof("get data index:%d exceptSeqID:%d", client.now, exceptSeqID)
						break
					}
					log.Errorf("ring bug! index:%d seqID:%d exceptSeqID:%d", client.now, client.ring[client.now].BinLog.SeqID, exceptSeqID)
				}
				log.Infof("waiting for data... index:%d exceptSeqID:%d", client.now, exceptSeqID)
				time.Sleep(client.cfg.TimeSleep)
				continue
			}
			client.channel <- client.ring[client.now]
			client.ring[client.now] = nil

			client.now++
			if client.now == client.cfg.RingLen {
				client.now = 0
				client.batter = (client.batter + uint64(config.RingLen)) % global.MaxSeqID
			}

			exceptSeqID++
			if exceptSeqID > global.MaxSeqID {
				exceptSeqID = global.MinSeqID
			}
		}
	}()

	return client, nil
}

// batter/0    batter+now-1/now-1  batter-len+now/now      batter-1/len-1
func (k *KafkaConsumer) NewPartitionMessgae(pid int32, offset int64) (*PartitionMessage, error) {
	cp, err := k.c.ConsumePartition(k.cfg.Topic, pid, offset)
	if err != nil {
		return nil, err
	}

	pm := &PartitionMessage{
		consumer: cp,
	}
	go func() {
		for msg := range pm.consumer.Messages() {
			if k.closed {
				break
			}
			binlog := new(global.BinLogData)
			_ = json.Unmarshal(msg.Value, binlog)
			bMsg := &ConsumerMessage{
				Msg:    msg,
				BinLog: binlog,
			}
			for {
				// deal with > maxSeqID
				// if maxSeqID >> ringLen then that is right
				// ignore when seqID in (now-9*ringLen, now)
				log.Infof("batter %d now %d binlog:%d ringLen:%d min: %d max: %d", k.batter, k.now, binlog.SeqID, k.cfg.RingLen, global.MinSeqID, global.MaxSeqID)
				if k.batter+uint64(k.now) > global.MaxSeqID {
					if k.batter-uint64(k.cfg.RingLen-k.now) > binlog.SeqID && binlog.SeqID > k.batter-uint64(10*k.cfg.RingLen-k.now) {
						break
					}

					if binlog.SeqID >= k.batter {
						k.ring[binlog.SeqID-k.batter] = bMsg
						log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, binlog.SeqID-k.batter)
						break
					} else if binlog.SeqID < k.batter+uint64(k.now)-global.MaxSeqID {
						k.ring[global.MaxSeqID-k.batter+binlog.SeqID] = bMsg
						log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, global.MaxSeqID-k.batter+binlog.SeqID)
						break
					} else if binlog.SeqID >= k.batter-uint64(k.cfg.RingLen-k.now+1) {
						k.ring[k.cfg.RingLen-int(k.batter-binlog.SeqID)] = bMsg
						log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, k.cfg.RingLen-int(k.batter-binlog.SeqID))
						break
					} else {
						time.Sleep(k.cfg.TimeSleep)
					}

					// batter 0 now 0 binlog:10000 ringLen:500 min: 1 max: 10000
				} else if k.batter < uint64(k.cfg.RingLen-k.now+1) {
					if k.batter+global.MaxSeqID-uint64(k.cfg.RingLen-k.now) > binlog.SeqID && binlog.SeqID > k.batter+global.MaxSeqID-uint64(10*k.cfg.RingLen-k.now) {
						break
					}

					if k.batter > binlog.SeqID {
						k.ring[k.cfg.RingLen-int(k.batter-binlog.SeqID)] = bMsg
						log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, k.cfg.RingLen-int(k.batter-binlog.SeqID))
						break
					} else if k.batter+uint64(k.now) > binlog.SeqID {
						k.ring[binlog.SeqID-k.batter] = bMsg
						log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, binlog.SeqID-k.batter)
						break
					} else if k.batter+global.MaxSeqID-uint64(k.cfg.RingLen-k.now) <= binlog.SeqID {
						k.ring[k.cfg.RingLen-int(k.batter+global.MaxSeqID-binlog.SeqID)] = bMsg
						log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, k.cfg.RingLen-int(k.batter+global.MaxSeqID-binlog.SeqID))
						break
					} else {
						time.Sleep(k.cfg.TimeSleep)
					}

				} else {
					if k.batter-uint64(k.cfg.RingLen-k.now) > binlog.SeqID {
						break
					}
					if k.batter-uint64(k.cfg.RingLen-k.now) < uint64(k.cfg.RingLen) && k.batter-uint64(10*k.cfg.RingLen-k.now)+global.MaxSeqID < binlog.SeqID {
						break
					}

					if binlog.SeqID < k.batter+uint64(k.now) {
						if k.batter > binlog.SeqID {
							k.ring[k.cfg.RingLen-int(k.batter-binlog.SeqID)] = bMsg
							log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, k.cfg.RingLen-int(k.batter-binlog.SeqID))
						} else {
							k.ring[binlog.SeqID-k.batter] = bMsg
							log.Debugf("binlog msg: seqID:%d pos:%d", binlog.SeqID, binlog.SeqID-k.batter)
						}
						break
					}
					time.Sleep(k.cfg.TimeSleep)
				}
			}
		}
	}()
	return pm, nil
}

func (k *KafkaConsumer) Message() <-chan *ConsumerMessage {
	return k.channel
}

func (k *KafkaConsumer) Callback(cm *ConsumerMessage) {
	k.offsetInfo.SequenceID = cm.BinLog.SeqID
	k.offsetInfo.PartitionOffset[cm.Msg.Partition] = cm.Msg.Offset
	return
}

// Close .
func (k *KafkaConsumer) Close() {
	if k.closed {
		return
	}
	k.closed = true
	for _, pc := range k.partitionConsumerArray {
		_ = pc.consumer.Close()
	}
	_ = k.c.Close()
	_ = k.offsetInfo.Save()
	return
}
