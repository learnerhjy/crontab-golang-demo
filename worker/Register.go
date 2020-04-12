package worker

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"net"
	"time"
)

type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	localIp string //本机Ipv4地址
}

var(
	G_Register *Register
)

// 获取本机网卡IpV4地址
func getLocalIp()(Ipv4 string,err error){
	var(
		addrs []net.Addr
		addr net.Addr
		IpNet *net.IPNet
		isIpNet bool
	)

	// 获得所有i网卡
	if addrs,err = net.InterfaceAddrs();err!=nil{
		return
	}

	// 找到第一个非loopback的网卡Ip
	for _,addr = range addrs{
		if IpNet,isIpNet = addr.(*net.IPNet); isIpNet && !IpNet.IP.IsLoopback(){
			// 跳过Ipv6
			if IpNet.IP.To4() != nil{
				Ipv4 = IpNet.IP.String()
				return
			}
		}
	}

	err = common.NO_LOCAL_IP_FOUND_ERROR
	return
}

// 注册到/cron/workers/Ip, 并自动续租
func (Register *Register) keepOnline() {
	var (
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		keepAliveChan <- chan *clientv3.LeaseKeepAliveResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
	)

	for {
		// 注册路径
		regKey = common.JOB_WORKER_DIR + Register.localIp

		cancelFunc = nil

		// 创建租约
		if leaseGrantResp, err = Register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		// 自动续租
		if keepAliveChan, err = Register.lease.KeepAlive(context.TODO(), leaseGrantResp.ID); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		// 注册到etcd
		if _, err = Register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			goto RETRY
		}

		// 处理续租应答
		for {
			select {
			case keepAliveResp = <- keepAliveChan:
				if keepAliveResp == nil {	// 续租失败
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

func InitRegister() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
		localIp string
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndPoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeOut) * time.Millisecond, // 连接超时
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}

	// 本机Ip
	if localIp, err = getLocalIp(); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_Register = &Register{
		client: client,
		kv: kv,
		lease: lease,
		localIp: localIp,
	}

	// 服务注册
	go G_Register.keepOnline()

	return
}