// Author: catlittlechen@gmail.com

package kafka

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"git.umlife.net/backend/mysql-bridge/global"
)

func TestRing(t *testing.T) {
	r := NewRingBuffer(1000, global.MinSeqID, time.Second)

	go func() {
		for {
			data := <-r.channel
			fmt.Println(data.BinLog.SeqID)
		}
	}()

	count := 3
	chans := make([]chan *ConsumerMessage, count)
	for i := 0; i < count; i++ {
		chans[i] = make(chan *ConsumerMessage, 1024)
	}
	real := make([]chan *ConsumerMessage, count)
	for i := 0; i < count; i++ {
		real[i] = make(chan *ConsumerMessage, 1024)
		go func(j int) {
			for cm := range real[j] {
				r.Set(cm)
			}
		}(i)
	}

	go func() {
		for i := global.MinSeqID; i < 1000000; i++ {
			time.Sleep(time.Microsecond)
			if i > global.MaxSeqID {
				i = global.MinSeqID
			}
			random := rand.Int() % count
			chans[random] <- &ConsumerMessage{
				BinLog: &global.BinLogData{
					SeqID: uint64(i),
				},
			}
		}
	}()

	go func() {
		for {
			random := rand.Int() % count
			select {
			case i := <-chans[0]:
				real[random] <- i
			case i := <-chans[1]:
				real[random] <- i
			case i := <-chans[2]:
				real[random] <- i
			}
		}
	}()

	time.Sleep(120 * time.Second)
	return
}
