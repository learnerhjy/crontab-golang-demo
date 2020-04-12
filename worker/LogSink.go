package worker

import (
	"context"
	"fmt"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"time"
)

type LogSink struct {
	client *mongo.Client
	logChan chan *common.JobLog // 任务日志队列
	collection *mongo.Collection
	autoCommitChan chan *common.JobLogBatch
}

var(
	G_LogSink *LogSink
)


// 初始化
func InitLogSink()(err error){
	// 建立连接
	var(
		client *mongo.Client
	)

	if client,err = mongo.Connect(context.TODO(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeOut)*time.Millisecond)); err!=nil{
		return
	}

	G_LogSink = &LogSink{
		client: client,
		collection:client.Database("cron").Collection("log"),
		logChan:make(chan*common.JobLog,1000),
		autoCommitChan:make(chan *common.JobLogBatch,1000),
	}

	go G_LogSink.writeLoop()

	return
}

// 将任务执行日志写到mongdb中
func(LogSink *LogSink)writeLoop(){
	var(
		jobLog *common.JobLog
		jobLogBatch *common.JobLogBatch
		autoCommitTimer *time.Timer // 自动提交计时器，设置为1s
		outTimeBatch *common.JobLogBatch // 过期批次
	)
	for{
		select {
		case jobLog =<- LogSink.logChan:
			if jobLogBatch == nil{
				jobLogBatch = &common.JobLogBatch{}
				// 如果jobLogBatch一直没满，也要保证log能够及时写入
				/*
					设定定时器，在指定的时间之后，向autoCommitChan中写入batch
				*/
				autoCommitTimer = time.AfterFunc(1000*time.Millisecond,
					func(batch *common.JobLogBatch) func(){
						// 无法自动提交 bug need fix
						return func(){
							LogSink.autoCommitChan <- batch
						}
					}(jobLogBatch),)


			}
			jobLogBatch.JobLogs = append(jobLogBatch.JobLogs,jobLog)
			if len(jobLogBatch.JobLogs)>=G_config.JobLogBatchSize{
				// 该批次已满，写入mongodb中，并清空
				fmt.Println("写入日志")
				LogSink.saveJobLog(jobLogBatch)
				jobLogBatch = nil
				autoCommitTimer.Stop()
			}
		case outTimeBatch =<- LogSink.autoCommitChan:
			// 有可能到期时批次恰好填满
			// 此时jobLogBatch 已经置为nil，而outTimeBatch还没有
			if outTimeBatch != jobLogBatch {
				continue
			}
			fmt.Println("自动提交日志")
			LogSink.saveJobLog(outTimeBatch)
			jobLogBatch = nil

		}
	}

}

// 保存日志到mongodb
func(LogSink *LogSink)saveJobLog(batch *common.JobLogBatch){
	LogSink.collection.InsertMany(context.TODO(), batch.JobLogs)
}

func(LogSink *LogSink)Append(log *common.JobLog){
	select {
	case LogSink.logChan <- log:
	default:// 满了丢弃
	}

}

