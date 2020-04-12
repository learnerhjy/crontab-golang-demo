package master

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"time"
)

type WorkerManager struct {
	Kv clientv3.KV
	Client *clientv3.Client
}

var(
	G_workerManager *WorkerManager
)

func(WorkerManager *WorkerManager)ListWorker()(IpArr []string,err error){
	var(
		getResp *clientv3.GetResponse
		kvpair *mvccpb.KeyValue
		workerIp string
	)
	IpArr = make([]string,0)

	if getResp,err = WorkerManager.Kv.Get(context.TODO(),common.JOB_WORKER_DIR,clientv3.WithPrefix());err!=nil{
		return
	}

	for _,kvpair = range getResp.Kvs{
		workerIp = common.ExtractWorkerIp(string(kvpair.Key))
		IpArr = append(IpArr,workerIp)
	}

	return
}


func InitWorkerManager()(err error){
	var(
		kv clientv3.KV
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

	G_workerManager = &WorkerManager{
		Kv:     kv,
		Client: client,
	}

	return
}
