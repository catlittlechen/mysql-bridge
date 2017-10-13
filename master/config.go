// Author: chenkai@youmi.net

package main

import (
	"errors"
	"io/ioutil"
	"strings"
	"time"

	"git.umlife.net/backend/mysql-bridge/kafka"
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
	Table    TableConfig               `yaml:"table"`
}

type MysqlConfig struct {
	Host     string `yaml:"host"`
	Port     uint16 `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// database@table
type TableConfig struct {
	Replication []string `yaml:"replication"`
	Prepar      []string `yaml:"prepar"`

	RepMap map[string]map[string]bool
	PreMap map[string]map[string]bool
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

	masterCfg.Table.PreMap = make(map[string]map[string]bool)
	masterCfg.Table.RepMap = make(map[string]map[string]bool)
	var database, table string
	for _, str := range masterCfg.Table.Prepar {
		array := strings.Split(str, "@")
		if len(array) != 2 {
			return errors.New("the format of prepar shoud be database@table")
		}
		database = array[0]
		table = array[1]
		if _, ok := masterCfg.Table.PreMap[database]; !ok {
			masterCfg.Table.PreMap[database] = make(map[string]bool)
		}
		masterCfg.Table.PreMap[database][table] = true
	}
	for _, str := range masterCfg.Table.Replication {
		array := strings.Split(str, "@")
		if len(array) != 2 {
			return errors.New("the format of prepar shoud be database@table")
		}
		database = array[0]
		table = array[1]
		if _, ok := masterCfg.Table.RepMap[database]; !ok {
			masterCfg.Table.RepMap[database] = make(map[string]bool)
		}
		masterCfg.Table.RepMap[database][table] = true
	}

	return nil
}
