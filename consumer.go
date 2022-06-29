package main

import (
	"consumer/Offsetmanager"
	"consumer/WorkPool"
	"fmt"
	"strconv"
	"time"
)

const (
	MAX_BUFFER_SIZE   = 100 //  缓冲区最大值
	SLEEP_TIME_BUFFER = 100 //  缓冲区满后休眠事件
)

var CallBackList = make(map[string]WorkPool.CallBack, 0) // 所有的数据处理函数
var BufferManager = Offsetmanager.NewBufferManager()

func addCallBack(key string, cb WorkPool.CallBack) {
	CallBackList[key] = cb
}

// 模拟数据到达
func OnData(data string) {
	for BufferManager.Size() >= MAX_BUFFER_SIZE {
		println("buffer full")
		time.Sleep(SLEEP_TIME_BUFFER * time.Millisecond)
	}
	bufferPtr := BufferManager.Push(data, uint32(len(CallBackList)))
	for key, foo := range CallBackList {
		WorkPool.AddTask(key, bufferPtr, foo) // task执行完成后会send2redis
	}
}

// 模拟数据处理函数
func test(a string) string {
	time.Sleep(time.Duration(1) * time.Second)
	return "这是处理后的数据"
}

func main() {
	WorkPool.SetBufferManager(BufferManager)
	for i := 0; i < 10; i++ {
		addCallBack(strconv.FormatInt(int64(i), 10), test)
	}
	WorkPool.Start(100)
	for i := 0; i < 100; i++ {
		go OnData("OnData:" + strconv.FormatInt(int64(i), 10))
	}
	time.Sleep(time.Duration(5) * time.Second)
	// fmt.Println(BufferManager.Size(), WorkPool.QueuedWork())
	for BufferManager.Size() > 0 {
		// fmt.Println(BufferManager.Size(), WorkPool.QueuedWork())
	}
	fmt.Println(BufferManager.Size(), WorkPool.QueuedWork())

}
