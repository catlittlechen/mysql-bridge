// Author: chenkai@youmi.net
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	logs "git.umlife.net/backend/mysql-bridge/log"
	log "github.com/sirupsen/logrus"
)

var (
	config        = flag.String("c", "./etc/config.yaml", "config")
	errorChan     = make(chan bool)
	channelSink   SinkAdapter
	channelSource SourceAdapter

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

	switch channelCfg.SinkType {
	case TCPType:
		channelSink = new(TCPSinkAdapter)
	case KafkaType:
		channelSink = new(KafkaSinkAdapter)
	case LogType:
		channelSink = new(LogSinkAdapter)
	}

	switch channelCfg.SourceType {
	case TCPType:
		channelSource = new(TCPSourceAdapter)
	case KafkaType:
		channelSource = new(KafkaSourceAdapter)
	}

	err = channelSink.New()
	if err != nil {
		log.Errorf("channelSink New failed. type:%d, err:%s", channelCfg.SinkType, err)
		return
	}

	err = channelSource.New(channelSink)
	if err != nil {
		log.Errorf("channelSource New failed. type:%d, err:%s", channelCfg.SourceType, err)
		return
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

	err = channelSink.Close()
	if err != nil {
		log.Errorf("channelSink close failed. err:%s", err)
	}
	err = channelSource.Close()
	if err != nil {
		log.Errorf("channelSource close failed. err:%s", err)
	}

	log.Error("ByeBye")

	return
}
