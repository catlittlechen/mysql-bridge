// Author: chenkai@youmi.net

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"git.umlife.net/backend/mysql-bridge/kafka"
	logs "git.umlife.net/backend/mysql-bridge/log"
	log "github.com/sirupsen/logrus"
)

var (
	config    = flag.String("c", "./etc/config.yaml", "config")
	kproducer *kafka.KafkaProducer
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
	// defer
	defer func() {
		syncer.Close()
		_ = kproducer.Close()
		_ = info.Close()
	}()

	go func() {
		serr := syncer.Run()
		if serr != nil {
			panic("syncer run failed. err:" + serr.Error())
			return
		}
	}()

	// Deal with signal
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc

	return
}
