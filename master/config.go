// Author: chenkai@youmi.net

package main

import (
	"io/ioutil"
	"time"

	"git.umlife.net/backend/mysql-bridge/kafka"
	"github.com/siddontang/go-mysql/mysql"
	"gopkg.in/yaml.v2"
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
}

func ParseConfigFile(filepath string) error {
	masterCfg.MockArgs = make(map[string]interface{})
	if confile, err := ioutil.ReadFile(filepath); nil == err {
		if err = yaml.Unmarshal(confile, &masterCfg); nil != err {
			return err
		}
	} else {
		return err
	}

	mysql.ServerVersion = masterCfg.Mysql.ServerVersion
	return nil
}
