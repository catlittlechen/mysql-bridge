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

var (
	slaveCfg Config
)

// Config 主配置
type Config struct {
	Logconf  string                    `yaml:"log_config"`
	InfoDir  string                    `yaml:"info_dir"`
	ServerID uint32                    `yaml:"server_id"`
	Second   time.Duration             `yaml:"second"`
	Mysql    []MysqlConfig             `yaml:"mysql"`
	Redis    RedisConfig               `yaml:"redis"`
	Kafka    kafka.KafkaProducerConfig `yaml:"kafka"`
	Monitor  MonitorConfig             `yaml:"monitor"`
}

type MysqlConfig struct {
	Host             string      `yaml:"host"`
	Port             uint16      `yaml:"port"`
	User             string      `yaml:"user"`
	Password         string      `yaml:"password"`
	InfoFileName     string      `yaml:"info_file_name"`
	TargetkafkaTopic string      `yaml:"target_kafka_topic"`
	SeqKey           string      `yaml:"seq_key"`
	Table            TableConfig `yaml:"table"`
}

type RedisConfig struct {
	Host string `yaml:"host"`
	Db   int    `yaml:"db"`
}

// database@table
type TableConfig struct {
	Replication []string `yaml:"replication"`
	MaxSize     int      `yaml:"max_size"`

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

	var database, table string
	for index := range slaveCfg.Mysql {
		slaveCfg.Mysql[index].Table.RepMap = make(map[string][]*regexp.Regexp)
		for _, str := range slaveCfg.Mysql[index].Table.Replication {
			array := strings.Split(str, "@")
			if len(array) != 2 {
				return errors.New("the format of replication shoud be database@table")
			}
			database = array[0]
			table = array[1]
			if _, ok := slaveCfg.Mysql[index].Table.RepMap[database]; !ok {
				slaveCfg.Mysql[index].Table.RepMap[database] = make([]*regexp.Regexp, 0)
			}
			slaveCfg.Mysql[index].Table.RepMap[database] = append(slaveCfg.Mysql[index].Table.RepMap[database], regexp.MustCompile(table))
		}
	}

	return nil
}
