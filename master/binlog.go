// Author: chenkai@youmi.net

package main

import (
	"encoding/binary"
	"time"

	"github.com/siddontang/go-mysql/replication"
)

// doing
// TODO prepar

// rotate close + open + fileDescription
func NewFormatDescriptionEventData() []byte {

	timeStamp := time.Now().Unix()
	postHeaderLen := []byte{56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 92, 0, 4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0, 0, 0, 10, 10, 10, 25, 25, 0}
	data := make([]byte, 76+len(postHeaderLen)+1+4)

	// timestamp
	pos := 0
	binary.LittleEndian.PutUint32(data[pos:], uint32(timeStamp))
	pos += 4

	// eventType
	data[pos] = byte(replication.FORMAT_DESCRIPTION_EVENT)
	pos++

	// serverID
	binary.LittleEndian.PutUint32(data[pos:], masterCfg.Mysql.ServerID)
	pos += 4

	// eventSize
	binary.LittleEndian.PutUint32(data[pos:], 111)
	pos += 4

	// log position
	// 预留
	pos += 4

	// flag
	pos += 2

	// binlog Version
	binary.LittleEndian.PutUint16(data[pos:], 4)
	pos += 2

	copy(data[pos:pos+50], masterCfg.Mysql.ServerVersion)
	pos += 50

	binary.LittleEndian.PutUint32(data[pos:], uint32(time.Now().Unix()))
	pos += 4

	data[pos] = byte(19)
	pos++

	copy(data[pos:], postHeaderLen)
	return data
}

func WriteBinlogToFile(data []byte) error {
	return nil
}

// translation BEGIN + **** + XID
// rewrite binlog
func WriteBinlog() {

	binlogParser := replication.NewBinlogParser()
	for msg := range kconsumer.Message() {
		kconsumer.Callback(msg)
	}
	return
}
