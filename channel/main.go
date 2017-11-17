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
	kconsumer *kafka.KafkaConsumer
	kproducer *kafka.KafkaProducer
	errorChan = make(chan bool)

	showVersion          = flag.Bool("version", false, "显示当前版本")
	gitBranch, gitCommit string
)

func main() {
	flag.Parse()
	if *showVersion {
		fmt.Printf("Version: %s\nBranch: %s\nCommit: %s\n", VERSION, gitBranch, gitCommit)
		return
	}

	err := ParseConfigFile(*config)
	if err != nil {
		fmt.Printf("parse configFile failed. err:%s", err)
		return
	}
	fmt.Printf("config: %+v\n", channelCfg)

	// init log
	logs.ConfiglogrusrusWithFile(channelCfg.Logconf)

	// init kafka
	kconsumer, err = kafka.NewKafkaConsumer(channelCfg.KafkaConsumer)
	if err != nil {
		log.Errorf("new kafka conusmer failed. err:%s", err)
		return
	}
	log.Info("new kafka conusmer success")

	defer func() {
		kconsumer.Close()
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
	kconsumer.Save()

	return
}
