// Author: chenkai@youmi.net

package tcp

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"time"

	"github.com/silenceper/pool"
	log "github.com/sirupsen/logrus"
)

type Sink struct {
	cfg    SinkConfig
	closed bool

	pc pool.Pool
}

func NewSink(cfg SinkConfig) (*Sink, error) {
	client := new(Sink)
	client.cfg = cfg

	poolConfig := &pool.PoolConfig{
		InitialCap:  5,
		MaxCap:      30,
		Factory:     func() (interface{}, error) { return net.Dial("tcp", client.cfg.Address) },
		Close:       func(v interface{}) error { return v.(net.Conn).Close() },
		IdleTimeout: client.cfg.IdleTimeout,
	}
	var err error
	client.pc, err = pool.NewChannelPool(poolConfig)
	if err != nil {
		return client, err
	}

	return client, nil
}

// 有并发问题
func (client *Sink) Write(data []byte) (err error) {
	var v interface{}
	v, err = client.pc.Get()
	if err != nil {
		return
	}

	conn := v.(net.Conn)
	defer func() {
		if err == nil {
			_ = client.pc.Put(v)
		} else {
			_ = conn.Close()
		}
	}()

	err = conn.SetWriteDeadline(time.Now().Add(client.cfg.WriteTimeout))
	if err != nil {
		log.Errorf("conn SetWriteDeadline failed. err:%s", err)
		return
	}

	err = writePacket(conn, data)
	if err != nil {
		log.Errorf("writePacket failed. err:%s", err)
		return
	}

	err = conn.SetReadDeadline(time.Now().Add(client.cfg.ReadTimeout))
	if err != nil {
		log.Errorf("conn SetReadDeadline failed. err:%s", err)
		return
	}

	bufferReader := bufio.NewReaderSize(conn, 4096)
	var replyData []byte
	replyData, err = readPacket(bufferReader)
	if err != nil {
		log.Errorf("readPacket failed. err:%s", err)
		return
	}

	if !bytes.Equal(replyData, okData) {
		err = errors.New("reply not ok! -> " + string(replyData))
		return
	}

	return nil
}
