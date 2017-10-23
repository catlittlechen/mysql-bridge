// Author: chenkai@youmi.net
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"

	"git.umlife.net/backend/mysql-bridge/kafka"
	logs "git.umlife.net/backend/mysql-bridge/log"
	"github.com/siddontang/go-mysql/server"
	log "github.com/sirupsen/logrus"
)

var (
	config    = flag.String("c", "./etc/config.yaml", "config")
	kconsumer *kafka.KafkaConsumer
	errorChan = make(chan bool)
)

func main() {
	flag.Parse()

	err := ParseConfigFile(*config)
	if err != nil {
		fmt.Printf("parse configFile failed. err:%s", err)
		return
	}
	fmt.Printf("config: %+v\n", masterCfg)

	// init log
	logs.ConfiglogrusrusWithFile(masterCfg.Logconf)

	// init kafka
	kconsumer, err = kafka.NewKafkaConsumer(masterCfg.Kafka)
	if err != nil {
		log.Errorf("new kafka conusmer failed. err:%s", err)
		return
	}
	log.Info("new kafka conusmer success")
	defer kconsumer.Close()

	binLogWriter := NewBinLogWriter()
	// init binlog
	go func() {
		defer func() {
			rerr := recover()
			if rerr != nil {
				log.Errorf(string(debug.Stack()))
				errorChan <- true
			}
		}()
		err := binLogWriter.WriteBinlog()
		if err != nil {
			log.Errorf("write binlog failed. err:" + err.Error())
			errorChan <- true
		}
		return
	}()

	l, err := net.Listen("tcp", masterCfg.Mysql.Host+":"+strconv.Itoa(int(masterCfg.Mysql.Port)))
	if err != nil {
		log.Errorf("net listen failed. err:%s", err)
		return
	}
	log.Info("listen success")

	go func() {
		defer func() {
			rerr := recover()
			if rerr != nil {
				log.Errorf(string(debug.Stack()))
				errorChan <- true
			}
		}()
		for {
			c, err := l.Accept()
			if err != nil {
				log.Errorf("listen accept failed. err:%s", err)
				errorChan <- true
				return
			}

			mock := new(MockHandler)
			mock.Args = masterCfg.MockArgs

			// Create a connection with user root and an empty passowrd
			// We only an empty handler to handle command too
			conn, err := server.NewConn(c, masterCfg.Mysql.User, masterCfg.Mysql.Password, mock)
			if err != nil {
				log.Errorf("server newConn failed. err:%s", err)
				errorChan <- true
				return
			}
			log.Info("server newConn success")

			go func() {
				defer mock.Close()
				defer func() {
					rerr := recover()
					if rerr != nil {
						log.Errorf(string(debug.Stack()))
						errorChan <- true
					}
				}()
				for {
					err := conn.HandleCommand()
					if err != nil {
						log.Errorf("handlerCommand failed. err:%s", err)
						return
					}
				}
			}()
		}
	}()

	defer func() {
		kconsumer.Close()
		binLogWriter.Close()
		kconsumer.Save()
	}()

	// Deal with signal
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	select {
	case <-sc:
	case <-errorChan:
	}

	kconsumer.Close()
	binLogWriter.Close()
	kconsumer.Save()

	return
}
