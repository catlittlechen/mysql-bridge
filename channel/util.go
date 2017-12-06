// Author: chenkai@youmi.net
package main

import (
	"bytes"
	"compress/zlib"
	"io"
)

type Message struct {
	Topic string `json:"topic"`
	Data  []KV   `json:"data"`
}

type KV struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func ZlibDecode(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	b := bytes.NewReader(data)
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(buf, r)
	if err != nil {
		return nil, err
	}
	_ = r.Close()

	return buf.Bytes(), nil
}

func ZlibEncode(data []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w := zlib.NewWriter(buf)

	_, err := w.Write(data)
	if err != nil {
		return nil, err
	}
	_ = w.Close()
	return buf.Bytes(), nil
}
