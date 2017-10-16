// Author: chenkai@youmi.net

package kafka

import (
	"os"
	"path"
	"sync"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	"github.com/siddontang/go/ioutil2"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type OffsetInfo struct {
	sync.RWMutex

	PartitionOffset map[int32]int64 `toml:"PartitionOffset"`
	SequenceID      uint64          `toml:"sequenceid"`

	filePath     string
	lastSaveTime time.Time
}

func loadOffsetInfo(dataDir string, ticker time.Duration) (*OffsetInfo, error) {
	var m OffsetInfo
	m.PartitionOffset = make(map[int32]int64)

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = path.Join(dataDir, "offset.info")
	m.lastSaveTime = time.Now()
	log.Debugf("filePath: %s lastSaveTime:%d", m.filePath, m.lastSaveTime)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if !os.IsNotExist(err) {
		_ = f.Close()
		err = global.ParseYamlFile(m.filePath, &m)
		if err != nil {
			return nil, err
		}
	}

	go func() {
		// Save binlog pos
		log.Info("save binlog pos ticker:%d", ticker)
		ticker := time.Tick(ticker)
		for range ticker {
			log.Infof("%+v\n", m.PartitionOffset)
			err := m.Save()
			if err != nil {
				log.Errorf("OffsetInfo save failed. err:%s", err)
			}
		}
	}()

	return &m, nil
}

func (m *OffsetInfo) Save() error {
	m.Lock()
	defer m.Unlock()

	if len(m.filePath) == 0 {
		return nil
	}

	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	m.lastSaveTime = n
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}

	if err = ioutil2.WriteFileAtomic(m.filePath, data, 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}

	return err
}

func (m *OffsetInfo) Close() error {
	return m.Save()
}
