package WorkPool

import (
	"consumer/Offsetmanager"
	"sync"
	"time"

	"github.com/goinggo/workpool"
)

const (
	TASK_MAX_SIZE   = 500
	SLEEP_TIME_TASK = 100 // 100 ms
)

type CallBack func(string) string

type MyWork struct {
	key    string
	buffer *Offsetmanager.Buffer
	cb     CallBack
	WP     *workpool.WorkPool
}

var BufferManager *Offsetmanager.BufferManager
var taskMutex, popMutex sync.Mutex
var WorkPool *workpool.WorkPool

// 设置一下BufferManager，用于消费完后pop数据
func SetBufferManager(o *Offsetmanager.BufferManager) {
	BufferManager = o
}

// 模拟发送数据到Redis
func sendToRedis(data string) {
	// TODO: 要单独抽出来
	// fmt.Printf("send to redis : %s\n", data)
}

func (mw *MyWork) DoWork(workRoutine int) {

	if mw.cb != nil {
		retData := mw.cb(mw.buffer.Data)
		sendToRedis(retData)
	}
	popMutex.Lock()
	mw.buffer.Num++
	BufferManager.CheckAndUpdata() // 检查是否被消费
	popMutex.Unlock()
}

func AddTask(key string, buffer *Offsetmanager.Buffer, cb CallBack) {
	work := MyWork{
		key:    key,
		buffer: buffer,
		cb:     cb,
		WP:     WorkPool,
	}
	taskMutex.Lock()
	for WorkPool.QueuedWork() >= TASK_MAX_SIZE {
		time.Sleep(time.Duration(SLEEP_TIME_TASK) * time.Millisecond)
	}
	WorkPool.PostWork("routine", &work)
	taskMutex.Unlock()
}

func Start(threadPoolSize int) {
	WorkPool = workpool.New(threadPoolSize, TASK_MAX_SIZE)
}

func ActiveRoutines() int32 {
	return WorkPool.ActiveRoutines()
}

func QueuedWork() int32 {
	return WorkPool.QueuedWork()
}
