// Copyright 2017
// Author: chenkai@youmi.net
//         huangjunwei@youmi.net

package main

import (
	"context"
	"strings"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

// Syncer 读取MySQL binlog，写入Kafka
type Syncer struct {
	syncer     *replication.BinlogSyncer
	info       *masterInfo
	closed     bool
	runChannel chan bool
}

func NewSyncer(info *masterInfo) *Syncer {
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: slaveCfg.ServerID,
		Flavor:   "mysql",
		Host:     slaveCfg.Mysql.Host,
		Port:     slaveCfg.Mysql.Port,
		User:     slaveCfg.Mysql.User,
		Password: slaveCfg.Mysql.Password,
		Charset:  mysql.DEFAULT_CHARSET,
	}

	return &Syncer{
		syncer:     replication.NewBinlogSyncer(cfg),
		info:       info,
		closed:     false,
		runChannel: make(chan bool, 1),
	}
}

func (syncer *Syncer) Run() (err error) {
	defer func() {
		syncer.runChannel <- true
	}()
	// Start sync with sepcified binlog file and position
	var streamer *replication.BinlogStreamer
	streamer, err = syncer.syncer.StartSync(syncer.info.Position())
	if err != nil {
		return err
	}

	var (
		event *replication.BinlogEvent

		transaction [][]byte
		size        int

		shouldWrite      bool
		splitTransaction bool

		// master binlog info
		name = syncer.info.Name
	)

	for {
		if syncer.closed {
			return
		}
		event, err = streamer.GetEvent(context.Background())
		if err != nil {
			log.Errorf("Streamer GetEvent failed. err: %s", err.Error())
			return
		}
		GlobalMonitor.AddCount()
		GlobalMonitor.SetTimeStamp(event.Header.Timestamp)

		switch event.Header.EventType {
		case replication.QUERY_EVENT:
			qe := event.Event.(*replication.QueryEvent)
			if strings.EqualFold(strings.ToUpper(string(qe.Query)), "BEGIN") || transaction != nil || splitTransaction {
				transaction = append(transaction, event.RawData)
				size += len(event.RawData)

			} else if _, ok := slaveCfg.Table.RepMap[strings.ToLower(string(qe.Schema))]; ok {
				// DML语句
				err = syncer.record([][]byte{event.RawData}, name, event.Header.LogPos)
				if err != nil {
					log.Errorf("syncer record failed. binlog: %s, pos: %d, eventType: %s, err: %s", name, event.Header.LogPos, event.Header.EventType.String(), err.Error())
					return
				}
			}
		case replication.TABLE_MAP_EVENT:
			shouldWrite = false
			te := event.Event.(*replication.TableMapEvent)
			if _, ok := slaveCfg.Table.RepMap[strings.ToLower(string(te.Schema))]; ok {
				table := strings.ToLower(string(te.Table))
				for _, re := range slaveCfg.Table.RepMap[strings.ToLower(string(te.Schema))] {
					if re.MatchString(table) {
						transaction = append(transaction, event.RawData)
						size += len(event.RawData)
						shouldWrite = true
						break
					}
				}
			}
		case replication.XID_EVENT:
			// deal with commit, pop kafka
			if (transaction != nil && len(transaction) != 1) || splitTransaction {
				transaction = append(transaction, event.RawData)
				size += len(event.RawData)
				err = syncer.record(transaction, name, event.Header.LogPos)
				if err != nil {
					log.Errorf("syncer record failed. binlog: %s, pos: %d, eventType: %s, err: %s", name, event.Header.LogPos, event.Header.EventType.String(), err.Error())
					return
				}
			}

			transaction = nil
			size = 0
			shouldWrite = false
			splitTransaction = false

		case replication.ROTATE_EVENT:
			name = string(event.Event.(*replication.RotateEvent).NextLogName)
		case replication.FORMAT_DESCRIPTION_EVENT:
			// ignore
		default:
			if transaction == nil && !splitTransaction {
				err = syncer.record([][]byte{event.RawData}, name, event.Header.LogPos)
				if err != nil {
					log.Errorf("syncer record failed. binlog: %s, pos: %d, eventType: %s, err: %s", name, event.Header.LogPos, event.Header.EventType.String(), err.Error())
					return
				}
			} else if shouldWrite {
				transaction = append(transaction, event.RawData)
				size += len(event.RawData)
				if size > slaveCfg.Table.MaxSize {
					err = syncer.record(transaction, name, event.Header.LogPos)
					if err != nil {
						log.Errorf("syncer record failed. binlog: %s, pos: %d, eventType: %s, err: %s", name, event.Header.LogPos, event.Header.EventType.String(), err.Error())
						return
					}
					transaction = nil
					size = 0
					splitTransaction = true
				}
			}
		}
	}

	return
}

func (s *Syncer) record(dataList [][]byte, name string, pos uint32) (err error) {

	var (
		seqID uint64
		b     []byte
	)
	seqID, err = GetSeqID()
	if err != nil {
		log.Errorf("getSeqID failed. err:%s", err)
		return
	}

	data := global.NewBinLogData(seqID, dataList)
	b, err = data.Encode()
	if err != nil {
		log.Errorf("binlogData encode failed. err:%s", err)
		return
	}

	topic := slaveCfg.Table.ReplicationTopic

	err = kproducer.Send(topic, b)
	if err != nil {
		log.Errorf("kafka producer send failed. err:%s", err)
		derr := DescSeqID()
		if derr != nil {
			// not panic, but alerover
			panic(derr)
		}
		return
	}

	s.info.Name = name
	s.info.Pos = pos
	return nil
}

func (s *Syncer) Close() {
	if s.closed {
		return
	}
	s.closed = true
	s.syncer.Close()
	<-s.runChannel
	log.Infof("syncer close success..")
	return
}
