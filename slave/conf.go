// Author: chenkai@youmi.net
package main

import (
	"io/ioutil"
	"time"

	"git.umlife.net/backend/mysql-bridge/kafka"
	"gopkg.in/yaml.v2"
)

var slaveCfg Config

// Config
type Config struct {
	Logconf  string                    `yaml:"log_config"`
	InfoDir  string                    `yaml:"info_dir"`
	ServerID uint32                    `yaml:"server_id"`
	Second   time.Duration             `yaml:"second"`
	Mysql    MysqlConfig               `yaml:"mysql"`
	Redis    RedisConfig               `yaml:"redis"`
	Kafka    kafka.KafkaProducerConfig `yaml:"kafka"`
}

type MysqlConfig struct {
	Host     string `yaml:"host"`
	Port     uint16 `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type RedisConfig struct {
	Host string `yaml:"host"`
	Db   int    `yaml:"db"`
}

func ParseConfigFile(filepath string) error {
	if confile, err := ioutil.ReadFile(filepath); nil == err {
		if err = yaml.Unmarshal(confile, &slaveCfg); nil != err {
			return err
		}
	} else {
		return err
	}

	return nil
}
