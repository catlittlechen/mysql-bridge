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
	do(t, "0ZXvv70xf1dJD++/vTDvv71/26Pvv70j")
	do(t, "77+977+9Le+/vVhwR++/ve+/vWPvv71477+9EUxJ")
	do(t, "akg677+9Cu+/vUIN77+977+9Mu+/ve+/vSke77+9")
	do(t, "77+977+9H++/vQ9NKe+/vVPvv70377+9dxPvv70=")
	do(t, "UVhuHEjvv71ORO+/vWrvv73vv73vv70BLe+/vQ==")
	do(t, "77+9KAPvv73vv71ZS8+mNibvv73vv70E77+9SQ==")
	do(t, "77+977+9aEl+77+9RO+/ve+/ve+/ve+/vTrvv70pag==")
	do(t, "77+977+9WO+/vRXvv71Cy6Mv77+9Ee+/ve+/vXzvv70=")
}
