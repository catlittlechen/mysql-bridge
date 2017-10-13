// Author: chenkai@youmi.net

package main

func WriteRelayLog() {
	for msg := range kconsumer.Message() {
		_ = msg
	}
}
