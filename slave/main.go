// Author: chenkai@youmi.net

package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"git.umlife.net/backend/mysql-bridge/kafka"
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
		log.Errorf("parse configFile failed. err:%s", err)
		return
	}

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
		serr := syncer.Run()
		if serr != nil {
			log.Errorf("syncer run failed. err:%s", serr)
			return
		}
	}()

	// defer
	defer func() {
		syncer.Close()
		_ = kproducer.Close()
		_ = info.Close()
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
