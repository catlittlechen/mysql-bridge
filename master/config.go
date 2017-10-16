// Author: chenkai@youmi.net

package main

import (
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"git.umlife.net/backend/mysql-bridge/kafka"
	"github.com/siddontang/go-mysql/mysql"
)

var masterCfg Config

// Config
type Config struct {
	Logconf  string                    `yaml:"log_config"`
	Second   time.Duration             `yaml:"second"`
	Mysql    MysqlConfig               `yaml:"mysql"`
	Kafka    kafka.KafkaConsumerConfig `yaml:"kafka"`
	MockArgs map[string]interface{}    `yaml:"mockargs"`
}

type MysqlConfig struct {
	Host          string `yaml:"host"`
	Port          uint16 `yaml:"port"`
	User          string `yaml:"user"`
	Password      string `yaml:"password"`
	ServerID      uint32 `yaml:"server_id"`
	ServerVersion string `yaml:"server_version"`

	BinLogDir  string `yaml:"binlog_dir"`
	BinLogSize int64  `yaml:"binlog_size"`
}

func ParseConfigFile(filepath string) error {
	masterCfg.MockArgs = make(map[string]interface{})
	err := global.ParseYamlFile(filepath, &masterCfg)
	if err != nil {
		return err
	}

	mysql.ServerVersion = masterCfg.Mysql.ServerVersion
	if masterCfg.Kafka.RingLen == 0 {
		masterCfg.Kafka.RingLen = 10240
	}
	return nil
}
