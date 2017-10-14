// Author: chenkai@youmi.net
package main

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/log"
)

type MockHandler struct {
	Pos      uint32
	FileName string
	RedoLog  *os.File
	Args     map[string]interface{}
}

func (h *MockHandler) GetValue(key string) (value interface{}) {
	if key == "UNIX_TIMESTAMP()" {
		value = time.Now().Unix()
		return
	}

	var ok bool
	value, ok = h.Args[key]
	if !ok {
		value = "NULL"
	}
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
	log.Debugf("query string: %s", query)

	array := strings.Split(query, " ")
	switch array[0] {
	case "SELECT":
		if len(array) < 1 {
			return nil, nil
		}

		value := h.GetValue(array[1])
		r, err := mysql.BuildSimpleResultset([]string{array[1]}, [][]interface{}{
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
					value = h.GetValue(array[num])
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
	log.Debugf("dump data pos:[%d] filename:[%s]", h.Pos, h.FileName)

	filename := filepath.Join(masterCfg.Mysql.BinLogDir, h.FileName)
	var err error
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
		eventHeader = new(replication.EventHeader)
		hold        []byte
		data        []byte
		needLen     = replication.EventHeaderSize
		n           int
		err         error
	)

	// Header
	hold = make([]byte, replication.EventHeaderSize)
	for {
		data = make([]byte, needLen)
		n, err = h.RedoLog.Read(data)
		if err == nil {
			copy(hold[len(hold)-needLen:], data)
			break
		}

		if err != io.EOF {
			return nil, err
		}

		copy(hold[len(hold)-needLen:], data)
		needLen -= n
		time.Sleep(time.Second)
	}

	err = eventHeader.Decode(hold)
	if err != nil {
		return nil, err
	}

	needLen = int(eventHeader.EventSize) - replication.EventHeaderSize
	data = make([]byte, eventHeader.EventSize)
	copy(data, hold)
	hold = data
	for {
		data = make([]byte, needLen)
		n, err = h.RedoLog.Read(data)
		if err == nil {
			copy(hold[len(hold)-needLen:], data)
			break
		}

		if err != io.EOF {
			return nil, err
		}

		copy(hold[replication.EventHeaderSize-needLen:], data)
		needLen -= n
		time.Sleep(time.Second)
	}

	return hold, nil
}

func (h *MockHandler) HandleRegisterSlave(data []byte) error {
	return nil
}
