// Author: chenkai@youmi.net

package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
)

var (
	okData   = []byte("ok")
	failData = []byte("fail")
)

// maxData 2**32
func readPacket(br *bufio.Reader) ([]byte, error) {
	buf := new(bytes.Buffer)
	header := []byte{0, 0, 0, 0}
	_, err := io.ReadFull(br, header)
	if err != nil {
		return nil, err
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16 | uint32(header[3]<<24))
	if length < 1 {
		return nil, errors.New("invalid payload length " + strconv.Itoa(length))
	}

	n, err := io.CopyN(buf, br, int64(length))
	if err != nil {
		return nil, err
	}

	if n != int64(length) {
		return nil, errors.New("readPacket length no equal to request")
	}

	return buf.Bytes(), nil
}

func writePacket(conn net.Conn, data []byte) error {

	length := len(data) - 4
	header := []byte{0, 0, 0, 0}
	header[0] = byte(length)
	header[1] = byte(length >> 8)
	header[2] = byte(length >> 16)
	header[3] = byte(length >> 24)

	// write Header
	n, err := conn.Write(header)
	if err != nil {
		return err
	}
	if n != len(header) {
		return errors.New("writePacket length no equal to request")
	}

	// write data
	n, err = conn.Write(header)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("writePacket length no equal to request")
	}
	return nil
}
