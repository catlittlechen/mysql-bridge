// Author: catlittlechen@gmail.com

package main

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

var (
	ErrCheckSum = errors.New("checksum failed")
)

func ChangePositionAndCheckSum(data []byte, pos uint32) []byte {
	binary.LittleEndian.PutUint32(data[13:], pos)
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.ChecksumIEEE(data[0:len(data)-4]))
	return data
}

func CheckSum(data []byte) error {
	sum := crc32.ChecksumIEEE(data[0 : len(data)-4])
	check := binary.LittleEndian.Uint32(data[len(data)-4:])
	if sum != check {
		return ErrCheckSum
	}
	return nil
}
