// Author: chenkai@youmi.net
package main

import (
	"time"

	"git.umlife.net/backend/mysql-bridge/global"

	"github.com/garyburd/redigo/redis"
	log "github.com/sirupsen/logrus"
)

func NewRedisPool(host string, db int) *redis.Pool {
	redisPool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 1 * time.Hour,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				log.Errorf("[Redis] Dial erro: %v", err)
				return nil, err
			}
			if db != 0 {
				if _, err := c.Do("SELECT", db); err != nil {
					_ = c.Close()
					log.Errorf("[Redis] Select DB error: %v", err)
					return nil, err
				}
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return redisPool
}

var redisPool *redis.Pool

func InitRedis() error {
	redisPool = NewRedisPool(slaveCfg.Redis.Host, slaveCfg.Redis.Db)
	return nil
}

// TODO 如果独立部署这部分的逻辑做成分发器，可以开发出随意添加同步表的功能
func GetSeqID(key string) (uint64, error) {
	// TODO 做成事务

	conn := redisPool.Get()
	defer func() {
		_ = conn.Close()
	}()

	value, err := redis.Uint64(conn.Do("INCRBY", key, 1))
	if err != nil {
		return 0, err
	}

	if value > global.MaxSeqID {
		value = global.MinSeqID
	} else {
		return value, nil
	}

	_, err = conn.Do("SET", key, value)
	if err != nil {
		return 0, err
	}

	return value, nil
}

func DescSeqID(key string) error {
	conn := redisPool.Get()
	defer func() {
		_ = conn.Close()
	}()

	value, err := redis.Uint64(conn.Do("INCRBY", key, -1))
	if err != nil {
		return err
	}

	if value < global.MinSeqID {
		value = global.MaxSeqID
	} else {
		return nil
	}

	_, err = conn.Do("SET", key, value)
	if err != nil {
		return err
	}

	return nil
}
