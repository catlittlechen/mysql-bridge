// Author: chenkai@youmi.net
package main

import (
	"errors"
	"regexp"
	"strings"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"git.umlife.net/backend/mysql-bridge/kafka"
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
	Table    TableConfig               `yaml:"table"`
	Monitor  MonitorConfig             `yaml:"monitor"`
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

// database@table
type TableConfig struct {
	ReplicationTopic string   `yaml:"replication_topic"`
	Replication      []string `yaml:"replication"`
	MaxSize          int      `yaml:"max_size"`

	RepMap map[string][]*regexp.Regexp `yaml:"-"`
}

type MonitorConfig struct {
	Host     string `yaml:"host"`
	Port     uint16 `yaml:"port"`
	Interval int    `yaml:"interval"`
}

func ParseConfigFile(filepath string) error {
	err := global.ParseYamlFile(filepath, &slaveCfg)
	if err != nil {
		return err
	}

	slaveCfg.Table.RepMap = make(map[string][]*regexp.Regexp)
	var database, table string
	for _, str := range slaveCfg.Table.Replication {
		array := strings.Split(str, "@")
		if len(array) != 2 {
			return errors.New("the format of replication shoud be database@table")
		}
		database = array[0]
		table = array[1]
		if _, ok := slaveCfg.Table.RepMap[database]; !ok {
			slaveCfg.Table.RepMap[database] = make([]*regexp.Regexp, 0)
		}
		slaveCfg.Table.RepMap[database] = append(slaveCfg.Table.RepMap[database], regexp.MustCompile(table))
	}

	return nil
}
