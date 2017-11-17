// Author: chenkai@youmi.net

package tcp

import (
	"bufio"
	"net"

	log "github.com/sirupsen/logrus"
)

type Source struct {
	cfg    SourceConfig
	closed bool

	listener    net.Listener
	handlerFunc func([]byte) error
	errChannel  chan error
}

func NewSource(cfg SourceConfig, handlerFunc func([]byte) error) (*Source, error) {
	client := new(Source)
	client.cfg = cfg
	client.closed = false
	client.handlerFunc = handlerFunc
	client.errChannel = make(chan error)

	var err error
	client.listener, err = net.Listen("tcp", client.cfg.Address)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			conn, err := client.listener.Accept()
			if err != nil {
				client.errChannel <- err
				return
			}
			go client.handler(conn)
		}
	}()
	return client, nil
}

func (client *Source) handler(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()
	bufferReader := bufio.NewReaderSize(conn, 4096)

	for {
		data, err := readPacket(bufferReader)
		if err != nil {
			log.Errorf("client readPacket failed. err:%s", err)
			return
		}

		// deal with data
		err = client.handlerFunc(data)
		if err != nil {
			log.Error("deal with data failed. err:%s", err)

			werr := writePacket(conn, failData)
			if werr != nil {
				log.Errorf("client writePacket failed. err:%s", werr)
			}

			return
		}

		err = writePacket(conn, okData)
		if err != nil {
			log.Errorf("client writePacket failed. err:%s", err)
			return
		}
	}
}
