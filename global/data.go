// Author: chenkai@youmi.net

package global

import "encoding/json"

type BinLogData struct {
	SeqID uint64 `json:"seqid"`
	Data  []byte `json:"data"`
}

func NewBinLogData(seqID uint64, data []byte) *BinLogData {
	return &BinLogData{
		SeqID: seqID,
		Data:  data,
	}
}

func (data *BinLogData) Encode() ([]byte, error) {
	return json.Marshal(data)
}

func (data *BinLogData) Decode(b []byte) error {
	return json.Unmarshal(b, data)
}
