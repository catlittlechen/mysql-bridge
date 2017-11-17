// Author: chenkai@youmi.net

package tcp

import "time"

type SourceConfig struct {
	Address string `yaml:"address"`
}

type SinkConfig struct {
	Address      string        `yaml:"address"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
}
