package Offsetmanager

import (
	"sync"

	"github.com/liyue201/gostl/ds/queue"
)

type Buffer struct {
	Data   string
	Num    uint32
	Target uint32
}

type BufferManager struct {
	q *queue.Queue
}

func NewBufferManager() *BufferManager {
	return &BufferManager{queue.New(queue.WithGoroutineSafe())}
}

func (b *BufferManager) Init() {
	b.q = queue.New()
}

var pushMutex sync.Mutex

func (x *BufferManager) Push(data string, target uint32) *Buffer {
	ptr := &Buffer{Data: data, Num: 0, Target: target}
	pushMutex.Lock()
	x.q.Push(ptr)
	pushMutex.Unlock()
	return ptr
}

func (x *BufferManager) Pop() {
	pushMutex.Lock()
	x.q.Pop()
	pushMutex.Unlock()
}

func (x *BufferManager) CheckAndUpdata() {

	var buf = x.q.Front().(Buffer)
	if buf.Num == buf.Target {
		x.q.Pop() // 说明被消费掉了
	}
}

func (b *BufferManager) Size() int {
	return b.q.Size()
}
