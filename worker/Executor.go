package worker

import (
	"context"
	"github.com/learnerhjy/crontab-golang-demo/common"
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
		)
		result = &common.JobExecuteResult{
			Job: info.Job,
		}
		result.StartTime = time.Now()
		cmd = exec.CommandContext(context.TODO(), info.Job.Command)
		output, err = cmd.CombinedOutput()
		result.EndTime = time.Now()
		result.Output = output
		result.Err = err
		// 把任务执行结果放到任务执行队列中
		G_Scheduler.PushJobResult(result)
	}()

}