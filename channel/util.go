// Author: chenkai@youmi.net
package main

type Message struct {
	Topic string `json:"topic"`
	Data  []KV   `json:"data"`
}

type KV struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}
