// Author: chenkai@youmi.net

package main

import (
	"os"
	"path"
	"sync"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type masterInfo struct {
	sync.RWMutex `yaml:"-"`

	Name string `yaml:"bin_name"`
	Pos  uint32 `yaml:"bin_pos"`

	filePath     string
	lastSaveTime time.Time
}

func loadMasterInfo(dataDir string) (*masterInfo, error) {
	var m masterInfo

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = path.Join(dataDir, "master.info")
	m.lastSaveTime = time.Now()

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
		return &m, nil
	}
	_ = f.Close()

	err = global.ParseYamlFile(m.filePath, &m)
	if err != nil {
		return nil, err
	}

	go func() {
		// Save binlog pos
		ticker := time.Tick(slaveCfg.Second)
		for range ticker {
			mp := m.Position()
			if time.Now().Unix()%60 == 0 {
				log.Infof("Current position: %+v", mp)
			}
			err := m.Save(mp)
			if err != nil {
				log.Errorf("masterInfo save failed. err: %s", err.Error())
			}
		}
	}()

	return &m, nil
}

func (m *masterInfo) Save(pos mysql.Position) error {
	m.RLock()
	defer m.RUnlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	if len(m.filePath) == 0 {
		return nil
	}

	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	if err = ioutil2.WriteFileAtomic(m.filePath, data, 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}

	return err
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		m.Name,
		m.Pos,
	}
}

// Close .
func (m *masterInfo) Close() error {
	pos := m.Position()

	err := m.Save(pos)
	if err != nil {
		return err
	}
	log.Info("masterInfo save success")
	return nil
}
