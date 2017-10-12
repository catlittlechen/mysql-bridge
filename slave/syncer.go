// Author: chenkai@youmi.net
package main

import (
	"context"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
)

type Syncer struct {
	syncer *replication.BinlogSyncer
	closed bool
}

func NewSyncer() *Syncer {
	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: slaveCfg.ServerID,
		Flavor:   "mysql",
		Host:     slaveCfg.Mysql.Host,
		Port:     slaveCfg.Mysql.Port,
		User:     slaveCfg.Mysql.User,
		Password: slaveCfg.Mysql.Password,
	}

	return &Syncer{
		syncer: replication.NewBinlogSyncer(cfg),
		closed: false,
	}
}

func (s *Syncer) Run(info *masterInfo) (err error) {
	// Start sync with sepcified binlog file and position
	var streamer *replication.BinlogStreamer
	streamer, err = s.syncer.StartSync(info.Position())
	if err != nil {
		log.Errorf("syner start failed. err:%s", err)
		return
	}

	var ev *replication.BinlogEvent
	var seqID uint64
	var b []byte
	for {
		if s.closed {
			return
		}
		ev, err = streamer.GetEvent(context.Background())
		if err != nil {
			log.Errorf("streamer getEvent failed. err:%s", err)
			return
		}

		seqID, err = GetSeqID()
		if err != nil {
			log.Errorf("getSeqID failed. err:%s", err)
			return
		}

		data := global.NewBinLogData(seqID, ev.RawData)
		b, err = data.Encode()
		if err != nil {
			log.Errorf("binlogData encode failed. err:%s", err)
			return
		}

		err = kproducer.Send(slaveCfg.Kafka.Topic, b)
		if err != nil {
			log.Errorf("kafka producer send failed. err:%s", err)
			return
		}

		if ev.Header.EventType == replication.ROTATE_EVENT {
			info.Name = string(ev.Event.(*replication.RotateEvent).NextLogName)
		}
		info.Pos = ev.Header.LogPos
	}
	return
}

func (s *Syncer) Close() {
	if s.closed {
		return
	}
	s.syncer.Close()
	s.closed = true
	return
}
