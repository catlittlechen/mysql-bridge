// Author: chenkai@youmi.net

package main

import (
	"os"
	"testing"
)

func TestVerify(t *testing.T) {
	f, err := os.Open("./test/binlog-1508150709.log")
	if err != nil {
		t.Fatal(err)
		return
	}
	stat, err := f.Stat()
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Logf("size:%d\n", stat.Size())
	pos, err := verify("./test/binlog-1508150709.log")
	t.Logf("pos:%d, err:%s\n", pos, err)
	if int64(pos) != stat.Size() {
		t.Fatal("verify failed.")
		return
	}
	t.Logf("ok")
	return
}
