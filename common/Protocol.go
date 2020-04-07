package common

import (
	"encoding/json"
	"fmt"
	"github.com/gorhill/cronexpr"
	"strings"
	"time"
)

// 任务结构体
type Job struct {
	JobName string `json:"jobName"` 	// 任务名称
	Command string	`json:"command"`	// 命令
	CronExpr string	`json:"cronExpr"`	// cron表达式
}

// 任务调度计划
type JobSchedulePlan struct {
	Job *Job // 要调度的任务信息
	Expr *cronexpr.Expression // 解析过的cron表达式
	NextTime time.Time // 下一次的调度时间
}

// 返回信息
type JSONResp struct {
	Status int 	`json:"status"`
	Msg string	`json:"msg"`
	Data interface{} `json:"data"`
}

// Job事件
type JobEvent struct {
	Type int
	Job *Job
}

// 任务执行信息
type JobExecuteInfo struct {
	Job *Job
	PlanTime time.Time
	RealTime time.Time
}

// 任务执行结果
type JobExecuteResult struct{
	Job *Job
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}
// 构建返回的json数据
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

// 反序列化任务
func UnpackJob(bytes []byte)(err error,ret *Job){
	var(
		job *Job
	)

	job = &Job{}
	if err = json.Unmarshal(bytes,job);err != nil{
		return
	}
	ret = job
	return
}

// 构建Job Event
func BuildJobEvent(Type int,job *Job)(jobEvent *JobEvent){
	return &JobEvent{
		Type: Type,
		Job:  job,
	}
}

// 从Job Key中获得job的name
func ExtractJobName(jobKey string)(jobName string){
	return strings.TrimPrefix(jobKey,JOB_SAVE_DIR)
}

// 根据job构建任务执行计划
func BuildJobSchedulePlan(job *Job)(jobSchedulePlan *JobSchedulePlan,err error){
	var(
		expr *cronexpr.Expression
	)
	if expr,err = cronexpr.Parse(job.CronExpr);err!=nil{
		return
	}
	jobSchedulePlan = &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return
}

// 构建任务执行信息
func BuildJobExecuteInfo(jobSchedulePlan *JobSchedulePlan)(jobExecuteInfo *JobExecuteInfo){
	jobExecuteInfo = &JobExecuteInfo{
		Job:      jobSchedulePlan.Job,
		PlanTime: jobSchedulePlan.NextTime,
		RealTime: time.Now(),
	}
	return
}