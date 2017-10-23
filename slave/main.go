// Author: chenkai@youmi.net

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"git.umlife.net/backend/mysql-bridge/kafka"
	logs "git.umlife.net/backend/mysql-bridge/log"
	log "github.com/sirupsen/logrus"
)

var (
	config    = flag.String("c", "./etc/config.yaml", "config")
	kproducer *kafka.KafkaProducer
	errorChan = make(chan bool)
)

func main() {
	flag.Parse()

	err := ParseConfigFile(*config)
	if err != nil {
		fmt.Printf("parse configFile failed. err:%s\n", err)
		return
	}
	fmt.Printf("%+v", slaveCfg)

	// init log
	logs.ConfiglogrusrusWithFile(slaveCfg.Logconf)

	// Init redis
	err = InitRedis()
	if err != nil {
		log.Errorf("init redis failed. err:%s", err)
		return
	}

	// Init kafka
	kproducer, err = kafka.NewKafkaProducer(slaveCfg.Kafka)
	if err != nil {
		log.Errorf("init kafka failed. err:%s", err)
		return
	}

	// Load master mysql binlog info from file
	info, err := loadMasterInfo(slaveCfg.InfoDir)
	if err != nil {
		log.Errorf("loadMasterInfo failed. err:%s", err)
		return
	}

	// Init syncer
	syncer := NewSyncer(info)
	go func() {
		defer func() {
			rerr := recover()
			if rerr != nil {
				log.Errorf(string(debug.Stack()))
				errorChan <- true
			}
		}()
		serr := syncer.Run()
		if serr != nil {
			log.Errorf("syncer run failed. err:%s", serr)
			errorChan <- true
			return
		}
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

	syncer.Close()
	_ = kproducer.Close()
	_ = info.Close()

	return
}
