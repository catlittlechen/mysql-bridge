// Author: chenkai@youmi.net

package kafka

import (
	"strconv"

	log "github.com/sirupsen/logrus"
)

const (
	InitOffset int64 = -1
)

type qitem struct {
	offset int64
	left   *qitem
	right  *qitem
}

func CopyQItem(first *qitem) *qitem {
	return &qitem{
		offset: first.offset,
		left:   first.left,
		right:  first.right,
	}
}

type SQueue struct {
	expectOffset int64
	head         *qitem
	tail         *qitem
}

func InitSQueue(offset int64) *SQueue {
	return &SQueue{
		expectOffset: offset,
	}
}

func (sq *SQueue) Do(offset int64) int64 {
	if sq.expectOffset > offset {
		return InitOffset
	}
	if sq.expectOffset == offset {
		sq.expectOffset++
		for sq.head != nil {
			if sq.head.offset != sq.expectOffset {
				break
			}

			sq.expectOffset++
			sq.head = sq.head.right
			sq.head.left = nil
		}
		return sq.expectOffset - 1
	}

	now := sq.head
	for now != nil {
		if now.offset < offset {
			now = now.right
			continue
		}

		next := CopyQItem(now)
		now.offset = offset

		next.left = now
		now.right = next

		return InitOffset
	}

	if sq.head == nil {
		sq.head = &qitem{
			offset: offset,
		}
		sq.tail = sq.head
	} else {
		sq.tail.right = &qitem{
			offset: offset,
			left:   sq.tail,
		}
		sq.tail = sq.tail.right
	}

	return InitOffset
}

func (sq *SQueue) Print() {
	now := sq.head
	s := strconv.Itoa(int(sq.expectOffset)) + "\t"
	for now != nil {
		s += strconv.Itoa(int(now.offset)) + ", "
		now = now.right
	}
	log.Infof(s)
}
