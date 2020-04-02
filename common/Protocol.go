package common

import (
	"encoding/json"
	"fmt"
)

// 任务结构体
type Job struct {
	JobName string `json:"jobName"` 	// 任务名称
	Command string	`json:"command"`	// 命令
	CronExpr string	`json:"cronExpr"`	// cron表达式
}


// 返回信息
type JSONResp struct {
	Status int 	`json:"status"`
	Msg string	`json:"msg"`
	Data interface{} `json:"data"`
}

func BuildJSONResp(status int,msg string,data interface{})(bytes []byte,err error){
	var(
		jsonResp = JSONResp{}
	)
	jsonResp.Status = status
	jsonResp.Msg = msg
	jsonResp.Data = data

	if bytes,err = json.Marshal(jsonResp);err!=nil{
		fmt.Println(err)
	}
	return
}