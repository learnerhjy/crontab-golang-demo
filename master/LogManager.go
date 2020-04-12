package master

import (
	"context"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"github.com/mongodb/mongo-go-driver/mongo"
	"github.com/mongodb/mongo-go-driver/mongo/clientopt"
	"github.com/mongodb/mongo-go-driver/mongo/findopt"
	"time"
)

type LogManager struct {
	client *mongo.Client
	collection *mongo.Collection
}

var(
	G_logManager *LogManager
)


// 初始化
func InitLogManager()(err error){
	// 建立连接
	var(
		client *mongo.Client
	)

	if client,err = mongo.Connect(context.TODO(),
		G_config.MongodbUri,
		clientopt.ConnectTimeout(time.Duration(G_config.MongodbConnectTimeOut)*time.Millisecond)); err!=nil{
		return
	}

	G_logManager = &LogManager{
		client:    	client,
		collection: client.Database("cron").Collection("log"),
	}

	return
}

func(LogManager *LogManager)ListJobLog(jobName string,skip int,limit int)(err error,logArr []*common.JobLog){
	var(
		jobLogFilter *common.JobLogFilter
		jobLogSortCond *common.JobLogSortCond
		jobLog *common.JobLog
		cursor mongo.Cursor
	)

	logArr = make([]*common.JobLog,0)

	// 按任务名过滤
	jobLogFilter = &common.JobLogFilter{JobName:jobName}
	// 按开始时间倒序排序
	jobLogSortCond = &common.JobLogSortCond{SortCond:-1}

	if cursor,err = LogManager.collection.Find(
		context.TODO(),jobLogFilter,
		findopt.Sort(jobLogSortCond),
		findopt.Skip(int64(skip)),
		findopt.Limit(int64(limit)));err!=nil{
		return
	}

	// 延迟释放游标
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()){
		jobLog = &common.JobLog{}
		if err = cursor.Decode(jobLog);err!=nil{
			// 日志解码失败，跳过
			continue
		}
		logArr = append(logArr,jobLog)
	}

	return

}