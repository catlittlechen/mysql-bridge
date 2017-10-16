// Author: chenkai@youmi.net
package main

import (
	"errors"
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
	PreparTopic      string   `yaml:"prepar_topic"`
	Prepar           []string `yaml:"prepar"`

	RepMap map[string]map[string]bool
	PreMap map[string]map[string]bool
}

func ParseConfigFile(filepath string) error {
	err := global.ParseYamlFile(filepath, &slaveCfg)
	if err != nil {
		return err
	}

	slaveCfg.Table.PreMap = make(map[string]map[string]bool)
	slaveCfg.Table.RepMap = make(map[string]map[string]bool)
	var database, table string
	for _, str := range slaveCfg.Table.Prepar {
		array := strings.Split(str, "@")
		if len(array) != 2 {
			return errors.New("the format of prepar shoud be database@table")
		}
		database = array[0]
		table = array[1]
		if _, ok := slaveCfg.Table.PreMap[database]; !ok {
			slaveCfg.Table.PreMap[database] = make(map[string]bool)
		}
		slaveCfg.Table.PreMap[database][table] = true
	}
	for _, str := range slaveCfg.Table.Replication {
		array := strings.Split(str, "@")
		if len(array) != 2 {
			return errors.New("the format of prepar shoud be database@table")
		}
		database = array[0]
		table = array[1]
		if _, ok := slaveCfg.Table.RepMap[database]; !ok {
			slaveCfg.Table.RepMap[database] = make(map[string]bool)
		}
		slaveCfg.Table.RepMap[database][table] = true
	}

	return nil
}
