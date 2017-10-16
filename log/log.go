// Author: catlittlechen@gmail.com

package log

import (
	"fmt"
	"io/ioutil"

	"github.com/rifflock/lfshook"
	"github.com/sirupsen/logrus"
	yaml "gopkg.in/yaml.v2"
)

// logrusConfig logrus配置结构体(查看logrusrus.sample.yaml)
type logrusConfig struct {
	EnableStdout bool `yaml:"enable_stdout"`
	DisableColor bool `yaml:"disable_color"`
	Paths        struct {
		Panic string `yaml:"panic"`
		Fatal string `yaml:"fatal"`
		Error string `yaml:"error"`
		Warn  string `yaml:"warn"`
		Info  string `yaml:"info"`
		Debug string `yaml:"debug"`
	} `yaml:"paths"`
}

func configlogrusPath(config *logrusConfig) {
	// 按照level分文件打logrus
	pathMap := lfshook.PathMap{
		logrus.DebugLevel: config.Paths.Debug,
		logrus.InfoLevel:  config.Paths.Info,
		logrus.WarnLevel:  config.Paths.Warn,
		logrus.ErrorLevel: config.Paths.Error,
		logrus.FatalLevel: config.Paths.Fatal,
		logrus.PanicLevel: config.Paths.Panic,
	}

	// 没有设置此level的logrus文件路径, 则用更低等级的logrus文件, 以此类推, 没设置则不logrus
	var captureLevel logrus.Level
	hasCaptureLevel := false
	for _, level := range []logrus.Level{
		logrus.DebugLevel, logrus.InfoLevel, logrus.WarnLevel, logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel} {
		path, ok := pathMap[level]
		if ok && path != "" {
			captureLevel = level
			hasCaptureLevel = true
		} else {
			if hasCaptureLevel {
				pathMap[level] = pathMap[captureLevel]
			} else {
				delete(pathMap, level)
			}
		}
	}
	logrus.AddHook(lfshook.NewHook(pathMap))

	logrus.Info("Level Debug logrus file: ", pathMap[logrus.DebugLevel])
	logrus.Info("Level Info logrus file: ", pathMap[logrus.InfoLevel])
	logrus.Info("Level Warn logrus file: ", pathMap[logrus.WarnLevel])
	logrus.Info("Level Error logrus file: ", pathMap[logrus.ErrorLevel])
	logrus.Info("Level Fatal logrus file: ", pathMap[logrus.FatalLevel])
	logrus.Info("Level Panic logrus file: ", pathMap[logrus.PanicLevel])
}

// Configlogrusrus 根据传入的logrusConfig初始化logrusrus
func Configlogrusrus(config *logrusConfig) {
	configlogrusPath(config)

	// logrus to stdout
	if !config.EnableStdout {
		logrus.SetOutput(ioutil.Discard)
	}
	logrus.Info("logrus to stdout: ", config.EnableStdout)
}

// ConfiglogrusrusWithFile 加载配置文件，失败直接panic
func ConfiglogrusrusWithFile(path string) {
	// 读取配置文件
	var config logrusConfig
	data, err := ioutil.ReadFile(path)
	if err != nil {
		panic(fmt.Errorf("read config file err - %s", err.Error()))
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		panic(fmt.Errorf("parse config err - %s", err.Error()))
	}

	if config.DisableColor {
		logrus.SetFormatter(&NoColorTextFormatter{})
	}
	Configlogrusrus(&config)
}
