// Author: chenkai@youmi.net

package main

import (
	"context"
	"os"
	"time"

	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

var channelData = make(chan []byte, 0)

func mockSlave() {
	master, err := loadMasterInfo("./masterBinLog")
	if err != nil {
		log.Errorf("loadMasterInfo fail. err:%s", err)
		return
	}
	defer func() {
		_ = master.Close()
	}()
	go func() {
		ticker := time.Tick(time.Second)
		for range ticker {
			mp := master.Position()
			if time.Now().Unix()%60 == 0 {
				log.Infof("%+v\n", mp)
			}
			err := master.Save(mp)
			if err != nil {
				log.Errorf("masterInfo save failed. err:%s", err)
			}
		}
	}()

	// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
	// flavor is mysql or mariadb
	cfg := replication.BinlogSyncerConfig{
		ServerID: 101,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "1232123",
	}
	syncer := replication.NewBinlogSyncer(cfg)

	// Start sync with sepcified binlog file and position
	streamer, _ := syncer.StartSync(master.Position())

	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	for {
		ev, _ := streamer.GetEvent(context.Background())

		channelData <- ev.RawData
		// Dump event
		ev.Dump(os.Stdout)

		if ev.Header.EventType == replication.ROTATE_EVENT {
			master.Name = string(ev.Event.(*replication.RotateEvent).NextLogName)
		}
		master.Pos = ev.Header.LogPos
	}

}
