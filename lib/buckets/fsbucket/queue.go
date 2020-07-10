package fsbucket

import "sync"

type (
	queue struct {
		*sync.RWMutex
		buf []elem
	}

	elem struct {
		depth  int
		prefix string
		path   string
	}
)

func newQueue(n int) *queue {
	return &queue{
		RWMutex: new(sync.RWMutex),
		buf:     make([]elem, 0, n),
	}
}

func (q *queue) Len() int {
	return len(q.buf)
}

func (q *queue) Push(s elem) {
	q.Lock()
	q.buf = append(q.buf, s)
	q.Unlock()
}

func (q *queue) Pop() (s elem) {
	q.Lock()
	if len(q.buf) > 0 {
		s = q.buf[0]
		q.buf = q.buf[1:]
	}
	q.Unlock()

	return
}
