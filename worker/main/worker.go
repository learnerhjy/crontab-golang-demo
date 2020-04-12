package main

import (
	"flag"
	"fmt"
	"github.com/learnerhjy/crontab-golang-demo/worker"
	"runtime"
	"time"
)

// 配置文件名
var(
	configFile string
)
// 解析命令行参数
func initArgs(){
	// master -config ..
	// master -h
	// 解析到哪，参数名称，默认值，help
	flag.StringVar(&configFile,"config","./worker.json","指定worker.json")
	flag.Parse()
}
// 启动多个线程，线程数与cpu核数相同
func initEnv(){
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	var(
		err error
	)
	// 从命令行中解析配置文件
	initArgs()
	// 初始化线程
	initEnv()
	// 文件名来自命令行
	// 加载配置
	if err = worker.InitConfig(configFile);err!=nil{
		fmt.Println(err)
		return
	}
	// 初始化服务注册模块
	if err = worker.InitRegister();err!=nil{
		fmt.Println(err)
		return
	}
	// 初始化调度模块
	if err = worker.InitScheduler();err!=nil{
		fmt.Println(err)
		return
	}

	// 初始化日志写入模块
	if err = worker.InitLogSink();err!=nil{
		fmt.Println(err)
		return
	}


	// 初始化任务执行模块
	if err = worker.InitExecutor();err!=nil{
		fmt.Println(err)
		return
	}

	// 初始化任务管理模块
	if err = worker.InitJobManager();err!=nil{
		fmt.Println(err)
		return
	}

	for{
		time.Sleep(1*time.Second)
	}

	return
}
