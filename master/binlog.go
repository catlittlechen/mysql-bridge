// Author: chenkai@youmi.net

package main

import (
	"encoding/binary"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

type BinLogWriter struct {
	file       *os.File
	closed     bool
	runChannel chan bool
}

func NewBinLogWriter() *BinLogWriter {
	return &BinLogWriter{
		file:       nil,
		closed:     false,
		runChannel: make(chan bool, 1),
	}
}

func ChangePositionAndCheckSum(data []byte, pos uint32) []byte {
	binary.LittleEndian.PutUint32(data[13:], pos)
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.ChecksumIEEE(data[0:len(data)-4]))
	return data
}

// rotate close + open + fileDescription

func NewRotateEventData(filename string, first bool) []byte {
	_, filename = filepath.Split(filename)
	timeStamp := time.Now().Unix()
	if !first {
		timeStamp = 0
	}
	data := make([]byte, 31+len(filename))
	// 19 + 8 + * + 4

	// Timestamp
	pos := 0
	binary.LittleEndian.PutUint32(data[pos:], uint32(timeStamp))
	pos += 4

	// EventType
	data[pos] = byte(replication.ROTATE_EVENT)
	pos++

	// ServerID
	binary.LittleEndian.PutUint32(data[pos:], masterCfg.Mysql.ServerID)
	pos += 4

	// EventSize
	binary.LittleEndian.PutUint32(data[pos:], uint32(31+len(filename)))
	pos += 4

	// Log Position
	// 预留
	pos += 4

	// Flag
	pos += 2

	// postition
	binary.LittleEndian.PutUint64(data[pos:], 4)
	pos += 8

	copy(data[pos:], []byte(filename))
	pos += len(filename)

	return data
}

func (b *BinLogWriter) NewFormatDescriptionEventData() []byte {

	timeStamp := time.Now().Unix()
	postHeaderLen := []byte{56, 13, 0, 8, 0, 18, 0, 4, 4, 4, 4, 18, 0, 0, 92, 0, 4, 26, 8, 0, 0, 0, 8, 8, 8, 2, 0, 0, 0, 10, 10, 10, 25, 25, 0}
	data := make([]byte, 81+len(postHeaderLen))

	// Timestamp
	pos := 0
	binary.LittleEndian.PutUint32(data[pos:], uint32(timeStamp))
	pos += 4

	// EventType
	data[pos] = byte(replication.FORMAT_DESCRIPTION_EVENT)
	pos++

	// ServerID
	binary.LittleEndian.PutUint32(data[pos:], masterCfg.Mysql.ServerID)
	pos += 4

	// EventSize
	binary.LittleEndian.PutUint32(data[pos:], uint32(81+len(postHeaderLen)))
	pos += 4

	// Log Position
	// 预留
	pos += 4

	// Flag
	pos += 2

	// Binlog Version
	binary.LittleEndian.PutUint16(data[pos:], 4)
	pos += 2

	// Server Version
	copy(data[pos:pos+50], masterCfg.Mysql.ServerVersion)
	pos += 50

	// TimeStamp
	binary.LittleEndian.PutUint32(data[pos:], uint32(timeStamp))
	pos += 4

	// HeaderLen
	data[pos] = byte(19)
	pos++

	copy(data[pos:], postHeaderLen)
	pos += len(postHeaderLen)

	// checksum
	data[pos] = byte(1) // CRC32
	pos++

	binary.LittleEndian.PutUint32(data[pos:], crc32.ChecksumIEEE(data[0:pos]))
	return data
}

// translation BEGIN + **** + XID
// rewrite binlog
func (b *BinLogWriter) WriteBinlog() (err error) {
	defer func() {
		b.runChannel <- true
	}()

	err = os.MkdirAll(masterCfg.Mysql.BinLogDir, 0755)
	if err != nil {
		return
	}

	var fileInfos []os.FileInfo
	fileInfos, err = ioutil.ReadDir(masterCfg.Mysql.BinLogDir)
	if err != nil {
		return
	}

	var useFileInfos []string
	validBinLogFileName := regexp.MustCompile(`^binlog-[0-9]+.log$`)
	for _, fi := range fileInfos {
		if validBinLogFileName.MatchString(fi.Name()) {
			useFileInfos = append(useFileInfos, fi.Name())
		}
	}

	lastFileName := "binlog-" + strconv.FormatInt(time.Now().Unix(), 10) + ".log"
	if len(useFileInfos) != 0 {
		sort.Strings(useFileInfos)
		lastFileName = useFileInfos[len(useFileInfos)-1]
		b.file, err = os.OpenFile(filepath.Join(masterCfg.Mysql.BinLogDir, lastFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0643)
	} else {
		b.file, err = b.CreateNewBinLogFile(lastFileName)
	}
	if err != nil {
		return
	}

	var header *replication.EventHeader
	for msg := range kconsumer.Message() {
		if b.closed {
			break
		}
		if msg == nil {
			log.Warnf("kconsumer get msg is nil")
			break
		}
		var stat os.FileInfo
		stat, err = b.file.Stat()
		if err != nil {
			return
		}
		size := uint32(stat.Size())
		binLogList := msg.BinLog.Data
		length := 0
		for _, binlog := range binLogList {
			length += len(binlog)
		}
		data := make([]byte, length)
		length = 0
		for _, binlog := range binLogList {
			size += uint32(len(binlog))
			binlog = ChangePositionAndCheckSum(binlog, size)
			copy(data[length:], binlog)
			length += len(binlog)
		}
		_, _ = b.file.Write(data)
		_ = b.file.Sync()
		kconsumer.Callback(msg)

		header = new(replication.EventHeader)
		err = header.Decode(binLogList[len(binLogList)-1][:19])
		if err != nil {
			return
		}
		if header.EventType == replication.XID_EVENT {
			err = b.RotateFile()
			if err != nil {
				return
			}
		}

	}

	return
}

func (b *BinLogWriter) CreateNewBinLogFile(filename string) (file *os.File, err error) {
	file, err = os.OpenFile(filepath.Join(masterCfg.Mysql.BinLogDir, filename), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0642)
	if err != nil {
		return
	}
	data := b.NewFormatDescriptionEventData()
	data = ChangePositionAndCheckSum(data, uint32(len(data)+4))
	// binlog magic code....
	_, _ = file.Write([]byte{0, 0, 0, 0})
	_, _ = file.Write(data)
	return
}

func (b *BinLogWriter) RotateFile() (err error) {
	var stat os.FileInfo
	stat, err = b.file.Stat()
	if err != nil {
		return
	}

	size := stat.Size()
	if size > masterCfg.Mysql.BinLogSize {

		nextFileName := "binlog-" + strconv.FormatInt(time.Now().Unix(), 10) + ".log"
		data := NewRotateEventData(nextFileName, true)
		data = ChangePositionAndCheckSum(data, uint32(size)+uint32(len(data)))
		_, _ = b.file.Write(data)

		_ = b.file.Sync()
		_ = b.file.Close()

		b.file, err = b.CreateNewBinLogFile(nextFileName)
		if err != nil {
			return
		}
	}
	return
}

func (b *BinLogWriter) Close() {
	if b.closed {
		return
	}
	b.closed = true
	<-b.runChannel
	if b.file != nil {
		_ = b.file.Sync()
		_ = b.file.Close()
	}

	return
}
