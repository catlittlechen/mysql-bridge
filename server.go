// Author: chenkai@youmi.net
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/server"
)

type MockHandler struct {
	Post     uint32
	FileName string
	Args     map[string]string
}

func (h *MockHandler) UseDB(dbName string) error {
	return nil
}

func (h *MockHandler) HandleQuery(query string) (*mysql.Result, error) {
	fmt.Println(query)
	array := strings.Split(query, " ")
	var ok bool
	switch array[0] {
	case "SELECT":
		if len(array) > 1 {
			var value interface{}
			if array[1] == "UNIX_TIMESTAMP()" {
				value = time.Now().Unix()
			} else {
				value, ok = h.Args[array[1]]
				if !ok {
					value = "NULL"
				}
			}
			r, err := mysql.BuildSimpleResultset([]string{array[1]}, [][]interface{}{
				[]interface{}{value},
			}, false)
			if err != nil {
				return nil, err
			} else {
				return &mysql.Result{0, 0, 0, r}, nil
			}
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
				value := array[num]
				if strings.HasPrefix(value, "@@") {
					value, ok = h.Args[value]
					if !ok {
						value = "NULL"
					}
				}
				h.Args[key] = value
			}
			return nil, nil
		}
	case "SHOW":
		if len(array) > 1 && array[1] == "VARIABLES" {
			// TODO 改成配置
			if len(array) > 3 && array[2] == "LIKE" {
				fields := []string{"Variable_name", "Value"}
				var values [][]interface{}
				switch array[3] {
				case "'SERVER_ID'":
					values = [][]interface{}{[]interface{}{"server_id", 18000}}
				case "SERVER_UUID":
					values = [][]interface{}{[]interface{}{"server_uuid", "2857b32d-9e9a-11e7-a268-94de80cb4372"}}
				}
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
	return nil, fmt.Errorf("not supported now")
}

func (h *MockHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	return 0, 0, nil, fmt.Errorf("not supported now")
}

func (h *MockHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, fmt.Errorf("not supported now")
}

func (h *MockHandler) HandleStmtClose(context interface{}) error {
	return nil
}

func (h *MockHandler) HandleDump(data []byte) error {
	if len(data) < 11 {
		return errors.New("wrong format")
	}

	h.Post = binary.LittleEndian.Uint32(data[1:5])
	//mod := binary.LittleEndian.Uint16(data[5:7])
	//serverID := binary.LittleEndian.Uint32(data[7:11])
	h.FileName = string(data[11:])
	fmt.Printf("dump data %d %s\n", h.Post, h.FileName)
	return nil
}

func (h *MockHandler) HandleGetData() ([]byte, error) {
	data := <-channelData
	return data, nil
}

func (h *MockHandler) HandleRegisterSlave(data []byte) error {
	return nil
}

func main() {
	go mockSlave()

	l, err := net.Listen("tcp", "172.17.0.1:4000")
	if err != nil {
		fmt.Println(err)
		return
	}

	c, err := l.Accept()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create a connection with user root and an empty passowrd
	// We only an empty handler to handle command too

	mock := new(MockHandler)
	mock.Args = make(map[string]string)
	mock.Args["@@global.binlog_checksum"] = "NONE"
	mock.Args["@@GLOBAL.GTID_MODE"] = "OFF"
	conn, err := server.NewConn(c, "root", "1232123", mock)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		err := conn.HandleCommand()
		if err != nil {
			fmt.Println(err)
			if strings.Contains(err.Error(), "connection closed") {
				_ = c.Close()
				c, err = l.Accept()
				if err != nil {
					fmt.Println(err)
					return
				}
				conn, err = server.NewConn(c, "root", "1232123", mock)
				if err != nil {
					fmt.Println(err)
					return
				}
			} else {
				fmt.Println(err)
				return
			}
		}
	}
	return
}
