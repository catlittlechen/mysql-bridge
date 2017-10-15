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
)

func ChangePositionAndCheckSum(data []byte, pos uint32) []byte {
	binary.LittleEndian.PutUint32(data[13:], pos)
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.ChecksumIEEE(data[0:pos]))
	return data
}

// rotate close + open + fileDescription

func NewRotateEventData(filename string, first bool) []byte {
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
	binary.LittleEndian.PutUint32(data[pos:], uint32(32+len(filename)))
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

func NewFormatDescriptionEventData() []byte {

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
func WriteBinlog() (err error) {

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

	var file *os.File
	lastFileName := "binlog-" + strconv.FormatInt(time.Now().Unix(), 10) + ".log"
	if len(useFileInfos) != 0 {
		sort.Strings(useFileInfos)
		lastFileName = useFileInfos[len(useFileInfos)-1]
		file, err = os.OpenFile(filepath.Join(masterCfg.Mysql.BinLogDir, lastFileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0643)
	} else {
		file, err = CreateNewBinLogFile(lastFileName)
	}
	if err != nil {
		return
	}

	file, err = RotateFile(file)
	if err != nil {
		return
	}

	for msg := range kconsumer.Message() {
		stat, _ := file.Stat()
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
		}
		_, _ = file.Write(data)
		kconsumer.Callback(msg)

		file, err = RotateFile(file)
		if err != nil {
			return err
		}
	}
	return
}

func CreateNewBinLogFile(filename string) (file *os.File, err error) {
	file, err = os.OpenFile(filepath.Join(masterCfg.Mysql.BinLogDir, filename), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0642)
	if err != nil {
		return
	}
	data := NewFormatDescriptionEventData()
	data = ChangePositionAndCheckSum(data, uint32(len(data)+4))
	_, _ = file.Write(data)
	return
}

func RotateFile(file *os.File) (nextFile *os.File, err error) {
	stat, _ := file.Stat()
	size := stat.Size()
	if size > masterCfg.Mysql.BinLogSize {
		nextFileName := "binlog-" + strconv.FormatInt(time.Now().Unix(), 10) + ".log"
		nextFile, err = CreateNewBinLogFile(nextFileName)

		data := NewRotateEventData(nextFileName, true)
		data = ChangePositionAndCheckSum(data, uint32(size)+uint32(len(data)))
		_, _ = file.Write(data)

		data = NewRotateEventData(nextFileName, false)
		data = ChangePositionAndCheckSum(data, 0)
		_, _ = file.Write(data)

		_ = file.Close()

	} else {
		nextFile = file
	}
	return
}
