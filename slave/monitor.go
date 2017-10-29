package main

import (
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Monitor 状态监控
type Monitor struct {
	cfg MonitorConfig

	preCount uint64
	count    uint64
	avgCount uint64

	timestamp uint32

	dataLock *sync.RWMutex
}

// GlobalMonitor monitor
var GlobalMonitor *Monitor

func InitMonitorWithConfig(cfg MonitorConfig) {

	GlobalMonitor = &Monitor{
		cfg:      cfg,
		dataLock: new(sync.RWMutex),
	}
	go GlobalMonitor.calculator()

	go func() {
		log.Infof("start monitor on %s:%d", cfg.Host, cfg.Port)
		gin.SetMode(gin.ReleaseMode)
		router := gin.New()
		router.GET("/info", GlobalMonitor.InfoHandler)
		router.Run(cfg.Host + ":" + strconv.Itoa(int(cfg.Port)))
	}()

	return
}

func (monitor *Monitor) AddCount() {
	monitor.dataLock.Lock()
	monitor.count++
	monitor.dataLock.Unlock()
}

func (monitor *Monitor) SetTimeStamp(now uint32) {
	monitor.dataLock.Lock()
	monitor.timestamp = now
	monitor.dataLock.Unlock()
}

// calculator 统计监控数据
func (monitor *Monitor) calculator() {
	fmt.Printf("%+v\n", monitor.cfg)
	t := time.Tick(time.Duration(monitor.cfg.Interval) * time.Second)
	for {
		select {
		case <-t:
			monitor.dataLock.Lock()
			monitor.preCount = monitor.count
			monitor.avgCount = (monitor.count - monitor.preCount) / uint64(monitor.cfg.Interval)
			monitor.dataLock.Unlock()
		}
	}

}

// InfoHandler backend状态http接口
func (monitor *Monitor) InfoHandler(c *gin.Context) {

	memstats := new(runtime.MemStats)
	runtime.ReadMemStats(memstats)

	result := fmt.Sprintf(
		"count: %d\n"+
			"avg_count: %d\n"+
			"timestamp: %d\n"+
			"Go Version: %s\n"+
			"Goroutine Num: %d\n"+
			"NumCPU: %d\n"+
			"NumCgoCall: %d\n"+
			"Memstats Alloc: %d\n"+
			"MemStats TotalAlloc: %d\n"+
			"MemStats Sys: %d\n"+
			"MemStats Lookups: %d\n"+
			"MemStats Mallocs: %d\n"+
			"MemStats Frees: %d\n"+
			"MemStats StackInuse: %d\n"+
			"MemStats StackSys: %d\n"+
			"MemStats HeepAlloc: %d\n"+
			"MemStats HeepSys: %d\n"+
			"MemStats HeepIdle: %d\n"+
			"MemStats HeepInuse: %d\n"+
			"MemStats HeepReleased: %d\n"+
			"MemStats HeepObjects: %d\n"+
			"MemStats NextGC: %d\n"+
			"MemStats LastGC: %s\n"+
			"MemStats PauseTotalNs: %d\n"+
			"MemStats EnableGC: %t\n"+
			"MemStats NumGC: %d\n"+
			"MemStats GCCPUFraction: %f\n",
		monitor.count,
		monitor.avgCount,
		monitor.timestamp,
		runtime.Version(),
		runtime.NumGoroutine(),
		runtime.NumCPU(),
		runtime.NumCgoCall(),
		memstats.Alloc,
		memstats.TotalAlloc,
		memstats.Sys,
		memstats.Lookups,
		memstats.Mallocs,
		memstats.Frees,
		memstats.StackInuse,
		memstats.StackSys,
		memstats.HeapAlloc,
		memstats.HeapSys,
		memstats.HeapIdle,
		memstats.HeapInuse,
		memstats.HeapReleased,
		memstats.HeapObjects,
		memstats.NextGC,
		time.Unix(int64(memstats.LastGC), 0).Format("2006/01/02:15:04:05"),
		memstats.PauseTotalNs,
		memstats.EnableGC,
		memstats.NumGC,
		memstats.GCCPUFraction,
	)

	c.String(http.StatusOK, result)
}
