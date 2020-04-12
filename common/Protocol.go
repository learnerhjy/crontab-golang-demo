package common

import (
	"context"
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
	CancelFunc context.CancelFunc // 取消函数
	CancelCtx context.Context // 上下文
}

// 任务执行结果
type JobExecuteResult struct{
	ExecuteInfo *JobExecuteInfo
	Job *Job
	Output []byte
	Err error
	StartTime time.Time
	EndTime time.Time
}

// 任务执行日志
type JobLog struct{
	JobName string `json:"jobName" bson:"jobName"`
	Command string `json:"command" bson:"command"`
	Err string `json:"error" bson:"error"`
	Output string `json:"output" bson:"output"`
	PlanTime int64 `json:"planTime" bson:"planTime"`          // 任务计划调度时间
	ScheduleTime int64 `json:"scheduleTime" bson:"scheduleTime"`  // 任务实际调度时间
	StartTime int64 `json:"startTime" bson:"startTime"`        // 任务执行开始时间
	EndTime int64 `json:"endTime" bson:"endTime"`			  // 任务执行结束时间
}

// 任务执行日志批次
type JobLogBatch struct {
	JobLogs []interface{}
}

// 日志过滤条件
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

// 日志排序条件
type JobLogSortCond struct{
	SortCond int `bson:"startTime"`
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

// 从Job Key中获得要杀死的job的name
func ExtractKillerName(jobKey string)(jobName string){
	return strings.TrimPrefix(jobKey,JOB_KILL_DIR)
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
	jobExecuteInfo.CancelCtx,jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

// 从Key中获得worker的Ip地址
func ExtractWorkerIp(workerKey string)(workerIp string){
	return strings.TrimPrefix(workerKey,JOB_WORKER_DIR)
}