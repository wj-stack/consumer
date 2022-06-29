



// 模拟数据到达
func OnData(data string) {
	// 单线程版本： 等待消耗掉
	for key, foo := range CallBackList {
		fmt.Printf("%s : begin consume\n", key)
		ret := foo(data)
		fmt.Printf("%s : end consume\n", key)
		sendToRedis(ret)
	}
	Offsetmanager.Commit()
}
