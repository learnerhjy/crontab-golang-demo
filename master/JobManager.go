package master

import (
	"context"
	"encoding/json"
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
		Endpoints:   G_config.EndPoints,
		DialTimeout: time.Duration(G_config.DialTimeOut)*time.Millisecond,
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

	return
}

// 保存任务到etcd 返回旧任务(如果存在的话)
func (JobManager *JobManager)SaveJob(job *common.Job)(err error,oldJob *common.Job){
	var(
		JobKey string
		JobValue []byte
		putResp *clientv3.PutResponse
	)
	oldJob = &common.Job{}
	// value 为job序列化后的值
	if JobValue,err = json.Marshal(job);err!=nil{
		return
	}
	JobKey = common.JOB_SAVE_DIR + job.JobName
	if putResp,err = JobManager.Kv.Put(context.TODO(),JobKey,string(JobValue),clientv3.WithPrevKV());err!=nil{
		return
	}

	// 如果旧的任务存在
	if putResp.PrevKv!=nil{
		if err = json.Unmarshal(putResp.PrevKv.Value,oldJob);err!=nil{
			err = nil
			return
		}
	}
	return
}

// 删除任务 如果任务存在，返回被删除的任务信息
func(JobManager *JobManager)DeleteJob(jobName string)(err error,oldJob *common.Job){
	var(
		jobKey string
		delResp *clientv3.DeleteResponse
	)
	oldJob = &common.Job{}
	jobKey = common.JOB_SAVE_DIR + jobName
	if delResp,err = JobManager.Kv.Delete(context.TODO(),jobKey,clientv3.WithPrevKV());err!=nil{
		return
	}
	if len(delResp.PrevKvs)!=0{
		if err = json.Unmarshal(delResp.PrevKvs[0].Value,oldJob);err != nil{
			err = nil
			return
		}
	}

	return
}

// 获取所有任务信息
func(JobManager *JobManager)ListJobs()(err error,jobList []*common.Job){
	var(
		jobDir string
		getResp *clientv3.GetResponse
		kvPair	*mvccpb.KeyValue
		job *common.Job
	)

	jobList = make([]*common.Job,0)
	jobDir = common.JOB_SAVE_DIR

	if getResp,err = JobManager.Kv.Get(context.TODO(),jobDir,clientv3.WithPrefix());err!=nil{
		return
	}

	for _,kvPair = range getResp.Kvs{
		// *
		job = &common.Job{}
		if err = json.Unmarshal(kvPair.Value,job);err!=nil{
			err = nil
			continue
		}
		jobList = append(jobList,job)
	}

	//fmt.Println(len(jobList),"hhh")

	return
}

// 杀死指定任务
// 把要杀死的任务放在 /cron/jobs/killer下，worker监听这个目录
// 为了不浪费etcd的资源，设置租约，到期自动删除
func(JobManager *JobManager)KillJob(jobName string)(err error){
	var(
		killKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
	)

	killKey = common.JOB_KILL_DIR + jobName

	// 设置一秒过期的租约
	if leaseGrantResp,err = JobManager.Client.Grant(context.TODO(),1);err!=nil{
		return
	}
	
	leaseId = leaseGrantResp.ID

	if _,err = JobManager.Kv.Put(context.TODO(),killKey,"",clientv3.WithLease(leaseId));err!=nil{
		return
	}


	return
}
