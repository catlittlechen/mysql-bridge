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
	configFile = flag.String("c", "etc/config.yaml", "config")
	kproducer  *kafka.KafkaProducer
	errorChan  = make(chan bool)

	showVersion          = flag.Bool("version", false, "显示当前版本")
	gitBranch, gitCommit string
)

func main() {
	flag.Parse()
	if *showVersion {
		fmt.Printf("Version: %s\nBranch: %s\nCommit: %s\n", VERSION, gitBranch, gitCommit)
		return
	}

	err := ParseConfigFile(*configFile)
	if err != nil {
		fmt.Printf("Parse config file failed. err: %s\n", err.Error())
		return
	}
	fmt.Printf("%+v\n", slaveCfg)

	// init log
	logs.ConfiglogrusrusWithFile(slaveCfg.Logconf)

	// init monitor
	InitMonitorWithConfig(slaveCfg.Monitor)

	// Init redis
	err = InitRedis()
	if err != nil {
		log.Errorf("init redis failed. err: %s", err.Error())
		return
	}

	// Init kafka
	kproducer, err = kafka.NewKafkaProducer(slaveCfg.Kafka)
	if err != nil {
		log.Errorf("init kafka failed. err: %s", err.Error())
		return
	}

	// Init syncer
	var syncerList []*Syncer
	for _, mysqlConfig := range slaveCfg.Mysql {
		syncer, err := NewSyncer(&mysqlConfig)
		if err != nil {
			log.Errorf("new syncer failed. err:%s", err.Error())
			return
		}
		syncerList = append(syncerList, syncer)
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
				log.Errorf("syncer run failed. err: %s", serr.Error())
				errorChan <- true
				return
			}
		}()
	}

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

	for _, syncer := range syncerList {
		syncer.Close()
	}
	_ = kproducer.Close()
	for _, syncer := range syncerList {
		_ = syncer.Save()
	}

	return
}
