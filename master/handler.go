// Author: chenkai@youmi.net
package main

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	log "github.com/sirupsen/logrus"
)

type MockHandler struct {
	Pos      uint32
	FileName string

	RedoLog *os.File
	NewFile bool

	Args map[string]interface{}

	eventHeader *replication.EventHeader
	headerByte  []byte
	dataByte    []byte
}

func NewMockHandler(kv map[string]interface{}) *MockHandler {
	obj := new(MockHandler)

	obj.Args = kv

	obj.eventHeader = new(replication.EventHeader)
	obj.headerByte = make([]byte, replication.EventHeaderSize)
	obj.dataByte = make([]byte, replication.EventHeaderSize)

	return obj
}

func (h *MockHandler) GetValue(key string) (value interface{}) {
	if key == "UNIX_TIMESTAMP()" {
		value = time.Now().Unix()
		return
	}

	if strings.HasPrefix(key, "@@") {
		key = key[2:]
	}

	var ok bool
	value, ok = h.Args[key]
	if !ok {
		value = "NULL"
	}
	log.Infof("Mock GetValue key %s, value:%s", key, value)
	return value
}

func (h *MockHandler) SetValue(key string, value interface{}) {
	h.Args[key] = value
	return
}

func (h *MockHandler) UseDB(dbName string) error {
	return nil
}

// all logic is designed for mock master mysql for replication
func (h *MockHandler) HandleQuery(query string) (*mysql.Result, error) {
	log.Infof("query string: %s", query)

	array := global.Split(query)
	switch array[0] {
	case "SELECT":
		if len(array) < 1 {
			return nil, global.ErrorSQLSyntax
		}

		value := h.GetValue(array[len(array)-1])
		r, err := mysql.BuildSimpleResultset([]string{array[len(array)-1]}, [][]interface{}{
			[]interface{}{value},
		}, false)
		if err != nil {
			return nil, err
		} else {
			return &mysql.Result{0, 0, 0, r}, nil
		}

	case "SET":
		if len(array) > 1 {
			key := array[1]
			num := 3
			if strings.HasSuffix(key, "=") {
				key = key[:len(key)-1]
				num = 2
			}
			if len(array) > num {
				var value interface{}
				if strings.HasPrefix(array[num], "@@") {
					value = h.GetValue(array[num][2:])
				} else {
					value = array[num]
				}
				h.Args[key] = value
			}
			return nil, nil
		}
	case "SHOW":
		if len(array) > 1 && array[1] == "VARIABLES" {
			if len(array) > 3 && array[2] == "LIKE" {
				fields := []string{"Variable_name", "Value"}
				array[3] = strings.ToLower(array[3])
				values := [][]interface{}{[]interface{}{array[3], h.Args[array[3]]}}
				r, err := mysql.BuildSimpleResultset(fields, values, false)
				if err != nil {
					return nil, err
				} else {
					return &mysql.Result{0, 0, 0, r}, nil
				}
			}
		}
	}
	return nil, nil
}

func (h *MockHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, errors.New("not supported now")
}

func (h *MockHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	return 0, 0, nil, errors.New("not supported now")
}

func (h *MockHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, errors.New("not supported now")
}

func (h *MockHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *MockHandler) HandleDump(data []byte) error {
	if len(data) < 11 {
		return errors.New("wrong format")
	}

	h.Pos = binary.LittleEndian.Uint32(data[1:5])
	//mod := binary.LittleEndian.Uint16(data[5:7])
	//serverID := binary.LittleEndian.Uint32(data[7:11])
	h.FileName = string(data[11:])
	if len(h.FileName) > 4 {
		index := strings.Index(h.FileName, ".log")
		h.FileName = h.FileName[:index+4]
	}
	log.Infof("dump data pos:[%d] filename:[%s]", h.Pos, h.FileName)

	// TODO
	var err error
	if h.FileName == "" {
		var fileInfos []os.FileInfo
		fileInfos, err = ioutil.ReadDir(masterCfg.Mysql.BinLogDir)
		if err != nil {
			return err
		}

		var useFileInfos []string
		validBinLogFileName := regexp.MustCompile(`^binlog-[0-9]+.log$`)
		for _, fi := range fileInfos {
			if validBinLogFileName.MatchString(fi.Name()) {
				useFileInfos = append(useFileInfos, fi.Name())
			}
		}
		sort.Strings(useFileInfos)
		h.FileName = useFileInfos[0]
		h.Pos = 4
		h.NewFile = true
	}
	if h.Pos <= 4 {
		h.Pos = 4
		h.NewFile = true
	}

	filename := filepath.Join(masterCfg.Mysql.BinLogDir, h.FileName)
	h.RedoLog, err = os.Open(filename)
	if err != nil {
		return err
	}

	_, err = h.RedoLog.Seek(int64(h.Pos), 1)
	if err != nil {
		return err
	}

	return nil
}

func (h *MockHandler) HandleGetData() ([]byte, error) {
	var (
		needLen = replication.EventHeaderSize
		n       int
		err     error
	)

	if h.NewFile {
		log.Infof("new file[%s] has been read", h.RedoLog.Name())
		data := NewRotateEventData(h.RedoLog.Name(), false)
		data = ChangePositionAndCheckSum(data, 0)
		h.NewFile = false
		log.Debugf("data: %+v", data)
		return data, nil
	}

	// Header
	for {
		n, err = h.RedoLog.Read(h.headerByte[len(h.headerByte)-needLen:])
		if err == nil {
			break
		}

		if err != io.EOF {
			return nil, err
		}

		needLen -= n
		time.Sleep(time.Second)
	}

	err = h.eventHeader.Decode(h.headerByte)
	if err != nil {
		return nil, err
	}
	log.Debugf("header:%+v\n", h.eventHeader)

	if int(h.eventHeader.EventSize) > len(h.dataByte) {
		log.Infof("dataByte in handler %d --> %d", len(h.dataByte), h.eventHeader.EventSize)
		log.Infof("obj %+v, data:%s", h.eventHeader, h.headerByte)
		h.dataByte = make([]byte, h.eventHeader.EventSize)
	}

	hold := h.dataByte[:h.eventHeader.EventSize]
	copy(hold, h.headerByte)
	needLen = int(h.eventHeader.EventSize) - replication.EventHeaderSize
	for {
		n, err = h.RedoLog.Read(hold[len(hold)-needLen:])
		if err == nil {
			break
		}

		if err != io.EOF {
			return nil, err
		}

		needLen -= n
		time.Sleep(time.Second)
	}

	if h.eventHeader.EventType == replication.ROTATE_EVENT {
		ev := new(replication.RotateEvent)
		err = ev.Decode(hold[replication.EventHeaderSize : len(hold)-4])
		if err != nil {
			return nil, err
		}

		filename := filepath.Join(masterCfg.Mysql.BinLogDir, string(ev.NextLogName))
		h.RedoLog, err = os.Open(filename)
		if err != nil {
			return nil, err
		}

		_, err = h.RedoLog.Seek(int64(ev.Position), 1)
		if err != nil {
			return nil, err
		}
		h.NewFile = true
	}

	err = CheckSum(hold)
	if err != nil {
		log.Errorf("checkSum failed. size:%d, header:%+v, data: %s, err:%s", len(hold), h.eventHeader, hold, err)
		return nil, err
	}
	log.Debugf("file:%s head.Size %d data:%d", h.RedoLog.Name(), h.eventHeader.EventSize, len(hold))

	return hold, nil
}

func (h *MockHandler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h *MockHandler) Close() {
	if h.RedoLog != nil {
		_ = h.RedoLog.Close()
	}
}
