// Author: chenkai@youmi.net
package main

import (
	"flag"
	"net"
	"strconv"

	"github.com/siddontang/go-mysql/server"
	log "github.com/sirupsen/logrus"
)

var mock_addr = flag.String("mock_addr", "172.17.0.1", "mock server addr")
var mock_port = flag.Int("mock_port", 4000, "mock server port")
var mock_user = flag.String("mock_user", "root", "mock server user")
var mock_password = flag.String("mock_password", "1232123", "mock server password")

func main() {
	flag.Parse()
	go mockSlave()

	l, err := net.Listen("tcp", *mock_addr+":"+strconv.Itoa(*mock_port))
	if err != nil {
		log.Errorf("net listen fail. err:%s", err)
		return
	}

	mock := new(MockHandler)
	mock.Args = make(map[string]interface{})
	mock.Args["server_id"] = 18001
	mock.Args["server_uuid"] = "2857b32d-9e9a-11e7-a268-94de80cb4372"
	mock.Args["@@global.binlog_checksum"] = "NONE"
	mock.Args["@@GLOBAL.GTID_MODE"] = "OFF"

	for {
		c, err := l.Accept()
		if err != nil {
			log.Errorf("listen accept fail. err:%s", err)
			return
		}

		// Create a connection with user root and an empty passowrd
		// We only an empty handler to handle command too
		conn, err := server.NewConn(c, *mock_user, *mock_password, mock)
		if err != nil {
			log.Errorf("server newConn fail. err:%s", err)
			return
		}

		go func() {
			defer conn.Close()
			for {
				err := conn.HandleCommand()
				if err != nil {
					log.Errorf("handlerCommand fail. err:%s", err)
					return
				}
			}
		}()
	}

	return
}
