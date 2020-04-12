package worker

import (
	"context"
	"fmt"
	"github.com/coreos/etcd/clientv3"
	"github.com/learnerhjy/crontab-golang-demo/common"
)

// 锁结构
type JobLock struct {
	kv clientv3.KV
	lease clientv3.Lease
	jobName string
	cancelFunc context.CancelFunc // 用于结束自动续约
	leaseId clientv3.LeaseID // 用于释放锁（释放租约）
	isLock bool
}

// 初始化锁
func InitJobLock(kv clientv3.KV,lease clientv3.Lease,jobName string)(joblock *JobLock){
	joblock = &JobLock{
		kv:      kv,
		lease:   lease,
		jobName: jobName,
	}
	return
}

// 尝试获取锁
func (JobLock *JobLock)TryLock()(err error){
	var(
		leaseResp *clientv3.LeaseGrantResponse
		leaseId clientv3.LeaseID
		leaseRespChan <- chan *clientv3.LeaseKeepAliveResponse
		cancelFunc context.CancelFunc
		ctx context.Context
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	// 用于取消续租
	ctx,cancelFunc = context.WithCancel(context.TODO())
	// 创建租约(5s)
	if leaseResp,err = JobLock.lease.Grant(context.TODO(),5);err!=nil{
		fmt.Println(err)
		return
	}
	leaseId = leaseResp.ID
	// 自动续租
	if leaseRespChan,err = JobLock.lease.KeepAlive(ctx,leaseId);err!=nil{
		fmt.Println(err)
		goto FAIL
	}

	// 处理自动续租的协程
	go func(){
		var(
			leaseKeepResp *clientv3.LeaseKeepAliveResponse
		)
		for{
			select {
			case leaseKeepResp = <-leaseRespChan:
				if leaseKeepResp == nil {
					fmt.Println("租约已到期")
					return
				} else {
					fmt.Println("续租成功", leaseKeepResp.Revision)
				}
			}
		}
	}()

	// txn事务去抢占锁
	txn = JobLock.kv.Txn(context.TODO())

	// key是否存在
	lockKey = common.JOB_LOCK_DIR + JobLock.jobName
	txn.If (clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseId))).
		Else(clientv3.OpGet(lockKey))

	if txnResp,err = txn.Commit();err!=nil{
		// 如果事务提交失败 释放锁
		goto FAIL
	}

	if !txnResp.Succeeded{
		// 事务提交成功，但抢锁失败
		err = common.LOCK_ALREADY_OCCUPIED_ERROR
		goto FAIL

	}
	// 抢锁成功，保存cancel函数，租约id，抢锁成功状态，正常返回
	JobLock.cancelFunc = cancelFunc
	JobLock.leaseId = leaseId
	JobLock.isLock = true
	return

FAIL:
	cancelFunc() // 取消自动续租
	JobLock.lease.Revoke(context.TODO(),leaseId) // 释放租约
	return
}

// 释放锁
func(JobLock *JobLock)UnLock(){
	if JobLock.isLock {
		JobLock.cancelFunc()
		JobLock.lease.Revoke(context.TODO(),JobLock.leaseId)
	}
}