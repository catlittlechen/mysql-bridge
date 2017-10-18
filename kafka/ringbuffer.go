package kafka

import (
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
	log "github.com/sirupsen/logrus"
)

type RingBuffer struct {
	length int
	sleep  time.Duration

	buffer []*ConsumerMessage

	now    int
	seqID  uint64 //min seqID
	batter uint64

	channel chan *ConsumerMessage
	closed  bool
}

func NewRingBuffer(length int, seqID uint64, sleep time.Duration) *RingBuffer {
	if seqID < global.MinSeqID {
		seqID = global.MinSeqID
	}
	r := &RingBuffer{
		length:  length,
		sleep:   sleep,
		buffer:  make([]*ConsumerMessage, length),
		now:     0,
		seqID:   seqID,
		batter:  seqID + uint64(length),
		channel: make(chan *ConsumerMessage, 1024),
	}
	if r.batter > global.MaxSeqID {
		r.batter = r.batter - global.MaxSeqID + global.MinSeqID - 1
	}
	go r.Run()
	return r
}

func (r *RingBuffer) Run() {
	for {
		if r.closed {
			return
		}
		if r.buffer[r.now] != nil {
			if r.buffer[r.now].BinLog.SeqID == r.seqID {
				log.Infof("get data index:%d expectSeqID:%d", r.now, r.seqID)
				break
			}
			log.Errorf("ring bug! index:%d seqID:%d expectSeqID:%d", r.now,
				r.buffer[r.now].BinLog.SeqID, r.seqID)
		}
		log.Infof("waiting for data... index:%d expectSeqID:%d", r.now, r.seqID)
	}

	r.channel <- r.buffer[r.now]
	r.buffer[r.now] = nil

	r.now++
	if r.now == r.length {
		r.now = 0
		r.batter = r.batter + uint64(r.length)
		if r.batter > global.MaxSeqID {
			r.batter = r.batter - global.MaxSeqID + global.MinSeqID - 1
		}
	}

	r.seqID++
	if r.seqID > global.MaxSeqID {
		r.seqID = global.MinSeqID
	}
	return
}

func (r *RingBuffer) Set(cm *ConsumerMessage) {
	log.Infof("batter %d now %d binlog:%d ringLen:%d min: %d max: %d", r.batter, r.now, cm.BinLog.SeqID, r.length, global.MinSeqID, global.MaxSeqID)

	for {
		// 抛弃某些奇怪的ID
		seqID := cm.BinLog.SeqID
		if seqID < r.seqID && global.MaxSeqID-global.MinSeqID+1 > 2*(r.seqID-seqID) {
			log.Warnf("duplicate %d seqID:%d min:%d max:%d", seqID, r.seqID, global.MinSeqID, global.MaxSeqID)
			break
		}

		// 比较的时候方便
		batter := r.batter
		if batter <= r.seqID {
			batter = batter + global.MaxSeqID - global.MinSeqID + 1
		}
		if seqID < r.seqID {
			seqID = seqID + global.MaxSeqID - global.MinSeqID + 1
		}

		if seqID < batter {
			r.buffer[r.length-int(batter-seqID)] = cm
			log.Infof("binlog msg: seqID:%d pos:%d", cm.BinLog.SeqID, r.length-int(batter-seqID))
			break
		}
		if seqID < r.seqID+uint64(r.length) {
			r.buffer[seqID-batter] = cm
			log.Infof("binlog msg: seqID:%d pos:%d", cm.BinLog.SeqID, seqID-batter)
			break
		}
		time.Sleep(r.sleep)
	}
	return
}

func (r *RingBuffer) Close() {
	if r.closed {
		return
	}
	r.closed = true
	return
}
