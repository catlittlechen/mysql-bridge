// Author: catlittlechen@gmail.com

package kafka

import "testing"
import "github.com/Shopify/sarama"
import "encoding/base64"

func do(t *testing.T, s string) {

	p := NewDefaultPartitioner("")
	msg := &sarama.ProducerMessage{}
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
		return
	}
	msg.Key = sarama.StringEncoder(data)
	var pid int32
	pid, err = p.Partition(msg, 6)
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Log(pid)
	return
}
func TestPartition(t *testing.T) {
	do(t, "qGAsmE6vQlu55MDuji9soQ==")

}
