package worker

import (
	"github.com/learnerhjy/crontab-golang-demo/common"
	"math/rand"
	"os/exec"
	"time"
)

type Executor struct{

}

var(
	G_Executor *Executor
)

func InitExecutor()(err error){
	G_Executor = &Executor{}
	return
}

func(Executor *Executor)ExecuteJob(info *common.JobExecuteInfo)(){
	go func() {
		var (
			cmd    *exec.Cmd
			output []byte
			err    error
			result *common.JobExecuteResult
			jobLock *JobLock
		)
		result = &common.JobExecuteResult{
			Job: info.Job,
			ExecuteInfo:info,
		}
		// 初始化分布式锁
		jobLock = G_jobManager.CreateJobLock(info.Job.JobName)
		result.StartTime = time.Now()
		// 抢锁前随机睡眠0~1s
		time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond)
		// 尝试抢锁
		err = jobLock.TryLock()
		defer jobLock.UnLock()
		if(err!=nil){
			// 失败
			result.Err = err
			result.EndTime = time.Now()
		}else{
			result.StartTime = time.Now()
			cmd = exec.CommandContext(info.CancelCtx, "/bin/bash", "-c",info.Job.Command)
			output, err = cmd.CombinedOutput()
			result.EndTime = time.Now()
			result.Output = output
			result.Err = err
		}

		// 把任务执行结果放到任务执行队列中
		// 抢锁失败也要执行，否则任务执行情况表中抢锁失败的任务不会被删除
		G_Scheduler.PushJobResult(result)
	}()

}