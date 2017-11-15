// Author: catlittlechen@gmail.com

package main

import (
	"os"

	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

func verify(filename string) (pos int, err error) {
	var redoLog *os.File
	redoLog, err = os.Open(filename)
	if err != nil {
		log.Errorf("verify open filename failed. err:%s", err)
		return
	}

	pos = 4
	_, err = redoLog.Seek(int64(pos), 1)
	if err != nil {
		log.Errorf("verify seek redolog failed. err:%s", err)
		return
	}

	var data []byte
	eventHeader := new(replication.EventHeader)
	for {
		var needLen = replication.EventHeaderSize
		// Header
		hold := make([]byte, replication.EventHeaderSize)
		data = make([]byte, needLen)
		_, err = redoLog.Read(data)
		if err != nil {
			log.Errorf("verify read redolog failed. err:%s", err)
			return
		}

		copy(hold[:], data)

		err = eventHeader.Decode(hold)
		if err != nil {
			log.Errorf("verify header decode failed. err:%s", err)
			return
		}
		log.Debug("header:%+v\n", eventHeader)

		needLen = int(eventHeader.EventSize) - replication.EventHeaderSize
		data = make([]byte, eventHeader.EventSize)
		copy(data, hold)
		hold = data
		data = make([]byte, needLen)
		_, err = redoLog.Read(data)
		if err != nil {
			log.Errorf("verify read redolog failed. err:%s", err)
			return
		}

		copy(hold[replication.EventHeaderSize:], data)
		err = CheckSum(hold)
		if err != nil {
			log.Errorf("verify read redolog failed. err:%s", err)
			return
		}

		pos += int(eventHeader.EventSize)
	}
	return
}
