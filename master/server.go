// Author: chenkai@youmi.net
package main

import (
	"flag"
	"net"
	"strconv"

	"git.umlife.net/backend/mysql-bridge/kafka"

	"github.com/siddontang/go-mysql/server"
	log "github.com/sirupsen/logrus"
)

var (
	config    = flag.String("c", "./etc/config.yaml", "config")
	kconsumer *kafka.KafkaConsumer
)

func main() {
	flag.Parse()

	err := ParseConfigFile(*config)
	if err != nil {
		log.Errorf("parse configFile failed. err:%s", err)
		return
	}

	// init kafka
	kconsumer, err = kafka.NewKafkaConsumer(masterCfg.Kafka)
	if err != nil {
		log.Errorf("new kafka conusmer failed. err:%s", err)
		return
	}
	defer kconsumer.Close()

	l, err := net.Listen("tcp", masterCfg.Mysql.Host+":"+strconv.Itoa(int(masterCfg.Mysql.Port)))
	if err != nil {
		log.Errorf("net listen failed. err:%s", err)
		return
	}

	mock := new(MockHandler)
	mock.Args = masterCfg.MockArgs
	/*
		mock.Args["server_id"] = 18001
		mock.Args["server_uuid"] = "2857b32d-9e9a-11e7-a268-94de80cb4372"
		mock.Args["@@global.binlog_checksum"] = "NONE"
		mock.Args["@@GLOBAL.GTID_MODE"] = "OFF"
	*/

	for {
		c, err := l.Accept()
		if err != nil {
			log.Errorf("listen accept failed. err:%s", err)
			return
		}

		// Create a connection with user root and an empty passowrd
		// We only an empty handler to handle command too
		conn, err := server.NewConn(c, masterCfg.Mysql.User, masterCfg.Mysql.Password, mock)
		if err != nil {
			log.Errorf("server newConn failed. err:%s", err)
			return
		}

		go func() {
			defer conn.Close()
			for {
				err := conn.HandleCommand()
				if err != nil {
					log.Errorf("handlerCommand failed. err:%s", err)
					return
				}
			}
		}()
	}

	return
}
