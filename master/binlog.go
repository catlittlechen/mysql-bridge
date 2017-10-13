// Author: chenkai@youmi.net

package main

// doing
// TODO prepar

// translation BEGIN + **** + XID
// rotate close + open + fileDescription

func NewBinLog() {

}

func WriteBinlog() {
	for msg := range kconsumer.Message() {
		kconsumer.Callback(msg)
	}
}
