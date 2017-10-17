// Author: chenkai@youmi.net

package main

import (
	"os"
	"testing"

	"github.com/siddontang/go-mysql/replication"
)

func TestHandler(t *testing.T) {
	parse := replication.NewBinlogParser()
	mock := new(MockHandler)
	mock.NewFile = true
	var err error
	mock.RedoLog, err = os.Open("./test/binlog-1508150709.log")
	if err != nil {
		t.Fatal(err)
		return
	}
	_, err = mock.RedoLog.Seek(4, 1)
	if err != nil {
		t.Fatal(err)
		return
	}

	times := 10
	for i := 0; i < times; i++ {
		data, err := mock.HandleGetData()
		if err != nil {
			t.Fatal(err)
			return
		}
		t.Logf("data: %+v\n", data)
		e, err := parse.Parse(data)
		if err != nil {
			t.Fatal(err)
			return
		}

		t.Logf("len:%d, header:%+v, event:%+v\n", len(data), e.Header, e.Event)
	}
	return
}
