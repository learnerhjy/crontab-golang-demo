package worker

import (
	"fmt"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"time"
)

// 任务调度
type Scheduler struct {
	jobEventChan chan *common.JobEvent // 任务调度队列
	jobSchedulePlanTable map[string]*common.JobSchedulePlan // 任务调度计划表，key为job的name，value为该job的执行计划
	// 该map与etcd中的任务信息同步
	jobExecutingTable map[string] *common.JobExecuteInfo // 任务执行信息表
	jobExecuteResultChan chan *common.JobExecuteResult // 任务执行结果队列
}

// 单例
var(
	G_Scheduler *Scheduler
)

// 处理任务执行结果
func(Scheduler *Scheduler)handleJobExecuteResult(result *common.JobExecuteResult){
	delete(Scheduler.jobExecutingTable,result.Job.JobName)
	fmt.Println("任务执行结果:",result.Job.JobName,":",string(result.Output))
}
// 处理任务调度计划，在任务调度计划表中增删
func(Scheduler *Scheduler)handleJobSchedulePlan(jobEvent *common.JobEvent)(err error){
	var(
		jobSchedulePlan *common.JobSchedulePlan
		job *common.Job
		jobIsExist bool
	)
	job = jobEvent.Job
	switch jobEvent.Type {
	// 保存事件，将任务执行计划存储到任务执行计划表中
	case common.JOB_SAVE_EVENT:
		if jobSchedulePlan,err = common.BuildJobSchedulePlan(job);err!=nil{
			return
		}
		Scheduler.jobSchedulePlanTable[job.JobName] = jobSchedulePlan
	// 删除事件，若任务存在于任务执行计划表中，删除
	case common.JOB_DELETE_EVENT:
		if jobSchedulePlan,jobIsExist = Scheduler.jobSchedulePlanTable[job.JobName];jobIsExist{
			delete(Scheduler.jobSchedulePlanTable,job.JobName)
		}
	}
	return
}

// 扫描任务调度计划表，执行已经过期的任务，并且找到过期时间离当前时间最近的任务
func (Scheduler *Scheduler)trySchedule()(duration time.Duration){
	var(
		now time.Time
		nearestTime *time.Time
		jobSchedulePlan *common.JobSchedulePlan
	)
	// 如果任务调度计划表为空，则返回1秒
	//fmt.Println(len(Scheduler.jobSchedulePlanTable))
	if len(Scheduler.jobSchedulePlanTable) == 0{
		duration = 1 * time.Second
		return
	}
	now = time.Now()
	for _,jobSchedulePlan = range Scheduler.jobSchedulePlanTable{
		// 如果任务的过期时间等于或早于当前时间，立即尝试执行（如果任务上一次还没执行完则不执行）
		if jobSchedulePlan.NextTime.Equal(now) || jobSchedulePlan.NextTime.Before(now) {
			//TODO
			fmt.Println("执行任务",jobSchedulePlan.Job.JobName)
			Scheduler.tryStartJob(jobSchedulePlan)
			// 设置下一次执行时间
			jobSchedulePlan.NextTime = jobSchedulePlan.Expr.Next(now)
		}
		// 比较，找出最近过期的任务
		if nearestTime == nil||jobSchedulePlan.NextTime.Before(*nearestTime){
			nearestTime = &jobSchedulePlan.NextTime
		}
	}
	duration = (*nearestTime).Sub(now)
	fmt.Println(duration)
	return
}

// 执行任务
func(Scheduler *Scheduler) tryStartJob(jobSchedulePlan *common.JobSchedulePlan){
	// 执行!= 调度
	var(
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting bool
	)

	for jobExecuteInfo,jobExecuting =  Scheduler.jobExecutingTable[jobSchedulePlan.Job.JobName];jobExecuting;{
		fmt.Println(jobSchedulePlan.Job.JobName,"任务正在执行")
		return
	}
	jobExecuteInfo = common.BuildJobExecuteInfo(jobSchedulePlan)
	// 将任务放到任务执行情况表
	Scheduler.jobExecutingTable[jobSchedulePlan.Job.JobName] = jobExecuteInfo
	// 执行任务
	G_Executor.ExecuteJob(jobExecuteInfo)


}

// 调度协程
func(Scheduler *Scheduler)scheduleLoop(){
	var(
		jobEvent *common.JobEvent
		err error
		schedulerAfter time.Duration
		schedulerTimer *time.Timer
		result *common.JobExecuteResult
	)
	// 初始化
	schedulerAfter = Scheduler.trySchedule()
	schedulerTimer = time.NewTimer(schedulerAfter)
	for{
		select{
		case jobEvent =<- Scheduler.jobEventChan:
			if err = Scheduler.handleJobSchedulePlan(jobEvent);err!=nil{
				fmt.Println(err)
			}
		case <-schedulerTimer.C:
		case result=<-Scheduler.jobExecuteResultChan:
			// 从任务执行队列中拿到任务执行结果，并删除任务执行情况表中的相应任务
			Scheduler.handleJobExecuteResult(result)
		}
		// 当有新的任务事件到来或者最近要过期的任务过期时，重新调度一次任务，并重置最近过期时间
		schedulerAfter = Scheduler.trySchedule()
		schedulerTimer.Reset(schedulerAfter)
	}
}

// 推送任务事件
func(Scheduler *Scheduler)PushJobEvent(jobEvent *common.JobEvent)(){
	Scheduler.jobEventChan <- jobEvent
}

// 初始化调度器
func InitScheduler()(err error){
	G_Scheduler = &Scheduler{
		jobEventChan:make(chan *common.JobEvent,1000),
		jobSchedulePlanTable:make(map[string]*common.JobSchedulePlan),
		jobExecutingTable:make(map[string]*common.JobExecuteInfo),
	}

	// 启动调度协程
	go G_Scheduler.scheduleLoop()
	return
}

func(Scheduler *Scheduler)PushJobResult(result *common.JobExecuteResult){
	Scheduler.jobExecuteResultChan <- result
}