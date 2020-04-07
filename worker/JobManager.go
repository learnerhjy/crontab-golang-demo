package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"time"
)

// 任务管理器
type JobManager struct {
	Kv clientv3.KV
	Lease clientv3.Lease
	Client *clientv3.Client
}

// 单例
var(
	G_jobManager *JobManager
)

func InitJobManager()(err error){
	var(
		kv clientv3.KV
		lease clientv3.Lease
		client *clientv3.Client
		config clientv3.Config
	)

	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndPoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeOut)*time.Millisecond,
	}

	if client,err = clientv3.New(config);err!=nil{
		fmt.Println(err)
		return
	}

	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_jobManager = &JobManager{
		Kv:     kv,
		Lease:  lease,
		Client: client,
	}

	G_jobManager.WatchJob()

	return
}

// 从etcd中获取任务 并监听后续变化
func(JobManager *JobManager)WatchJob()(err error){
	var(
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		job *common.Job
		watchStartRevision int64
		watcher clientv3.Watcher
		watchChan clientv3.WatchChan
		watchChanResp clientv3.WatchResponse
		event *clientv3.Event
		jobEvent *common.JobEvent
		jobName string

	)
	if getResp,err = JobManager.Kv.Get(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithPrefix());err!=nil{
		return
	}
	for _,kvpair = range getResp.Kvs{
		// 反序列化
		fmt.Println("here...")
		if err,job = common.UnpackJob(kvpair.Value);err == nil{
			// 构建SAVE Event
			jobEvent = common.BuildJobEvent(common.JOB_SAVE_EVENT,job)
			fmt.Println(*jobEvent)
			// TODO 推给Schedule协程
			G_Scheduler.PushJobEvent(jobEvent)
		}

	}
	// 获得当前Revision+1为开始监听的Revision
	watchStartRevision = getResp.Header.Revision + 1
	watcher = clientv3.NewWatcher(JobManager.Client)
	watchChan = watcher.Watch(context.TODO(),common.JOB_SAVE_DIR,clientv3.WithRev(watchStartRevision),clientv3.WithPrefix())
	for watchChanResp = range watchChan{
		for _,event = range watchChanResp.Events{
			switch event.Type {
			case mvccpb.PUT:
				// 构建SAVE Event
				if err,job = common.UnpackJob(event.Kv.Value);err!=nil{
					continue
				}
				jobEvent = common.BuildJobEvent(common.JOB_SAVE_EVENT,job)
				fmt.Println(*jobEvent)
			case mvccpb.DELETE:
				// 构建DELETE Event
				jobName = common.ExtractJobName(string(event.Kv.Key))
				job = &common.Job{
					JobName:  jobName,
				}
				jobEvent = common.BuildJobEvent(common.JOB_DELETE_EVENT,job)
				fmt.Println(*jobEvent)


			}
			// TODO 推给Schedule协程
			G_Scheduler.PushJobEvent(jobEvent)
		}
	}
	return
}

