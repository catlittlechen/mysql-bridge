// Author: catlittlechen@gmail.com

package kafka

import (
	"math/rand"
	"testing"
	"time"
)

func TestQueue(t *testing.T) {
	qs := InitSQueue(1)
	var i int64
	var count int64 = 1000
	array := make([]int64, count)
	for i = 1; i <= count; i++ {
		array[i-1] = i
	}
	rand.Seed(time.Now().Unix())

	for i = 1; i < count*count; i++ {
		left := rand.Int() % int(count)
		right := rand.Int() % int(count)
		tmp := array[left]
		array[left] = array[right]
		array[right] = tmp
	}

	t.Logf("%+v\n", array)
	for i = 0; i < count; i++ {
		ans := qs.Do(array[i])
		qs.Print()
		if ans != -1 {
			t.Logf("%d ", ans)
		}
	}
	return
}
