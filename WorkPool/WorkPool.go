package WorkPool

import (
	"consumer/Offsetmanager"
	"sync"
	"time"

	"github.com/goinggo/workpool"
)

const (
	TASK_MAX_SIZE = 500
	SLEEP_TIME    = 100 // 100 ms
)

type CallBack func(string) string

type MyWork struct {
	key    string
	buffer *Offsetmanager.Buffer
	cb     CallBack
	WP     *workpool.WorkPool
}

func sendToRedis(data string) {
	// TODO: 要单独抽出来
	// fmt.Printf("send to redis : %s\n", data)
}

var BufferManager *Offsetmanager.BufferManager

func SetBufferManager(o *Offsetmanager.BufferManager) {
	BufferManager = o
}

var numTest uint32 = 0
var taskMutex, popMutex sync.Mutex
var WorkPool *workpool.WorkPool

func (mw *MyWork) DoWork(workRoutine int) {

	if mw.cb != nil {
		retData := mw.cb(mw.buffer.Data)
		sendToRedis(retData) // 把数据存放到redis中
	}
	popMutex.Lock()
	numTest++
	mw.buffer.Num++
	// println("mw.data:", mw.buffer.Data, "key:", mw.key, "num:", mw.buffer.Num)
	if mw.buffer.Num == mw.buffer.Target {
		BufferManager.Pop()
	}
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
		time.Sleep(time.Duration(SLEEP_TIME) * time.Millisecond)
	}
	WorkPool.PostWork("routine", &work)
	taskMutex.Unlock()
}
func Start(threadPoolSize int) {
	// fmt.Println("cpu:", runtime.NumCPU())
	// runtime.GOMAXPROCS(runtime.NumCPU())
	WorkPool = workpool.New(threadPoolSize, TASK_MAX_SIZE)

}

func ActiveRoutines() int32 {
	return WorkPool.ActiveRoutines()
}

func QueuedWork() int32 {
	return int32(numTest)
	// return WorkPool.QueuedWork()
}
