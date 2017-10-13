// Author: chenkai@youmi.net

package main

import (
	"encoding/binary"
	"strings"

	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

// doing
// TODO prepar

// rotate close + open + fileDescription
func NewBinLog() {
	data := make(76 + len(lengthOf))
	binary.LittleEndian.PutUint16()
}

func WriteBinlogToFile(data []byte) error {
	return nil
}

// translation BEGIN + **** + XID
// rewrite binlog
func WriteBinlog() {

	var (
		shouldWrite     bool
		hasWritten      bool
		beginBinLogByte []byte
	)

	binlogParser := replication.NewBinlogParser()
	for msg := range kconsumer.Message() {
		event, err := binlogParser.Parse(msg.BinLog.Data)
		if err != nil {
			log.Errorf("binlogParser parse failed. err:%s", err)
			return
		}
		switch event.Header.EventType {
		case replication.QUERY_EVENT:
			qe := event.Event.(*replication.QueryEvent)
			if strings.EqualFold(strings.ToUpper(string(qe.Query)), "BEGIN") {
				beginBinLogByte = msg.BinLog.Data
			} else {
				err = WriteBinlogToFile(msg.BinLog.Data)
				if err != nil {
					log.Errorf("WriteBinlogToFile failed. err:%s", err)
					return
				}
			}
		case replication.TABLE_MAP_EVENT:
			te := event.Event.(*replication.TableMapEvent)
			if _, ok := masterCfg.Table.RepMap[strings.ToLower(string(te.Schema))]; ok {
				if masterCfg.Table.RepMap[strings.ToLower(string(te.Schema))][strings.ToLower(string(te.Table))] {
					if !hasWritten {
						err = WriteBinlogToFile(beginBinLogByte)
						if err != nil {
							log.Errorf("WriteBinlogToFile failed. err:%s", err)
							return
						}
						hasWritten = true

						err = WriteBinlogToFile(msg.BinLog.Data)
						if err != nil {
							log.Errorf("WriteBinlogToFile failed. err:%s", err)
							return
						}
					}
					shouldWrite = true
				} else {
					shouldWrite = false
				}
			} else {
				shouldWrite = false
			}
		case replication.XID_EVENT:
			// deal with commit, pop queue and write binlog
			if hasWritten {
				err = WriteBinlogToFile(msg.BinLog.Data)
				if err != nil {
					log.Errorf("WriteBinlogToFile failed. err:%s", err)
					return
				}
				hasWritten = true
			}

			hasWritten = false
			shouldWrite = false
			beginBinLogByte = nil

		case replication.ROTATE_EVENT, replication.FORMAT_DESCRIPTION_EVENT:
			// ignore
		default:
			if shouldWrite {
				err = WriteBinlogToFile(msg.BinLog.Data)
				if err != nil {
					log.Errorf("WriteBinlogToFile failed. err:%s", err)
					return
				}
			}
		}

		kconsumer.Callback(msg)
	}
	return
}
