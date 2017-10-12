// Author: chenkai@youmi.net

package kafka

import (
	"bytes"
	"os"
	"path"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/siddontang/go/ioutil2"
	log "github.com/sirupsen/logrus"
)

type OffsetInfo struct {
	sync.RWMutex

	PartitionOffset map[int32]int64

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

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	} else if os.IsNotExist(err) {
		return &m, nil
	}
	defer func() {
		_ = f.Close()
	}()

	_, err = toml.DecodeReader(f, &m)
	if err != nil {
		return nil, err
	}

	go func() {
		// Save binlog pos
		ticker := time.Tick(ticker)
		for range ticker {
			if time.Now().Unix()%60 == 0 {
				log.Infof("%+v\n", m.PartitionOffset)
			}
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
	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)

	_ = e.Encode(m)

	var err error
	if err = ioutil2.WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}

	return err
}

func (m *OffsetInfo) Close() error {
	return m.Save()
}
