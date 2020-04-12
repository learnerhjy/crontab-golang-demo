package master

import (
	"encoding/json"
	"fmt"
	"github.com/learnerhjy/crontab-golang-demo/common"
	"net"
	"net/http"
	"strconv"
	"time"
)

// 任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

// 构建apiserver 单例
var(
	G_apiServer  *ApiServer
)
// 保存任务接口
// 从前端获得json
// job:{"jobName":name,"command":command,"cronExpr":cronExpr}
// 将json反序列化到对象中，并保存到etcd
func handleJobSave(resp http.ResponseWriter,req *http.Request){
	var(
		postJob string
		job common.Job
		oldJob *common.Job
		bytes []byte
		err error
	)
	// 解析post表单
	if err=req.ParseForm();err!=nil{
		goto ERR
	}
	// 获取要创建的job信息
	postJob = req.PostForm.Get("job")
	fmt.Println(postJob)
	// 反序列化json到job对象中
	if err = json.Unmarshal([]byte(postJob),&job);err!=nil{
		goto ERR
	}
	if err,oldJob = G_jobManager.SaveJob(&job);err!=nil{
		goto ERR
	}
	// 返回成功信息
	if bytes,err = common.BuildJSONResp(0,"success",oldJob);err == nil{
		resp.Write(bytes)
		return
	}
ERR:
	if bytes,err = common.BuildJSONResp(-1,err.Error(),nil);err == nil{
		resp.Write(bytes)
	}


}


// 删除任务接口
// 从前端获得json
// {jobName:name}
func handleJobDelete(resp http.ResponseWriter,req *http.Request){
	var(
		err error
		jobName string
		oldJob *common.Job
		bytes []byte
	)
	// 解析post表单
	if err=req.ParseForm();err!=nil{
		goto ERR
	}
	// 获取要删除的job名称
	jobName = req.PostForm.Get("jobName")

	if err,oldJob = G_jobManager.DeleteJob(jobName);err!=nil{
		goto ERR
	}
	// 返回成功信息
	if bytes,err = common.BuildJSONResp(0,"success",oldJob);err == nil{
		resp.Write(bytes)
		return
	}
ERR:
	if bytes,err = common.BuildJSONResp(-1,err.Error(),nil);err == nil{
		resp.Write(bytes)
	}
}

// 获取任务列表接口
func handleJobList(resp http.ResponseWriter,req *http.Request){
	var(
		err error
		jobList []*common.Job
		bytes []byte
	)

	if err,jobList = G_jobManager.ListJobs();err!=nil{
		goto ERR
	}

	// 返回成功信息
	if bytes,err = common.BuildJSONResp(0,"success",jobList);err == nil{
		resp.Write(bytes)
		return
	}

ERR:
	if bytes,err = common.BuildJSONResp(-1,err.Error(),nil);err == nil{
		resp.Write(bytes)
	}
}

// 强制结束任务接口
func handleJobKill(resp http.ResponseWriter,req *http.Request){
	var(
		err error
		jobName string
		bytes []byte
	)

	// 解析post表单
	if err=req.ParseForm();err!=nil{
		goto ERR
	}
	// 获取要杀死的job名称
	jobName = req.PostForm.Get("jobName")

	if err = G_jobManager.KillJob(jobName);err!=nil{
		goto ERR
	}
	// 返回成功信息
	if bytes,err = common.BuildJSONResp(0,"success",nil);err == nil{
		resp.Write(bytes)
		return
	}



ERR:
	if bytes,err = common.BuildJSONResp(-1,err.Error(),nil);err == nil{
		resp.Write(bytes)
	}
}


// 查询任务日志接口
func handleJobLog(resp http.ResponseWriter,req *http.Request){
	var(
		bytes []byte
		err error
		logArray []*common.JobLog
		jobName string
		skipParam string
		limitParam string
		skip int
		limit int

	)

	if err=req.ParseForm();err!=nil{
		goto ERR
	}

	// /job/log?jobName= &limit= &skip=
	jobName = req.Form.Get("jobName")
	limitParam = req.Form.Get("limit")
	skipParam = req.Form.Get("skip")

	fmt.Println(jobName)
	if limit,err = strconv.Atoi(limitParam);err!=nil{
		limit = 20
	}

	if skip,err = strconv.Atoi(skipParam);err!=nil{
		skip = 0
	}

	if err,logArray = G_logManager.ListJobLog(jobName,skip,limit);err!=nil{
		goto ERR
	}

	if bytes,err = common.BuildJSONResp(0,"success",logArray);err == nil{
		resp.Write(bytes)
		return
	}



ERR:
	if bytes,err = common.BuildJSONResp(-1,err.Error(),nil);err == nil{
		resp.Write(bytes)
	}
}

// 查询健康worker节点接口
func handleWorkerList(resp http.ResponseWriter,req *http.Request){
	var(
		bytes []byte
		err error
		workerIp []string

	)

	if workerIp,err = G_workerManager.ListWorker();err!=nil{
		goto ERR
	}

	if bytes,err = common.BuildJSONResp(0,"success",workerIp);err == nil{
		resp.Write(bytes)
		return
	}



ERR:
	if bytes,err = common.BuildJSONResp(-1,err.Error(),nil);err == nil{
		resp.Write(bytes)
	}
}

// 初始化服务
func InitApiServer()(err error) {
	var(
		mux *http.ServeMux
		listener net.Listener
		httpServer *http.Server
		staticDir http.Dir
		staticDirHandler http.Handler
	)
	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save",handleJobSave)
	mux.HandleFunc("/job/delete",handleJobDelete)
	mux.HandleFunc("/job/list",handleJobList)
	mux.HandleFunc("/job/kill",handleJobKill)
	mux.HandleFunc("/job/log",handleJobLog)
	mux.HandleFunc("/worker/list",handleWorkerList)

	// 静态文件路由
	staticDir = http.Dir(G_config.WebRoot)
	staticDirHandler = http.FileServer(staticDir)
	mux.Handle("/",http.StripPrefix("/",staticDirHandler))

	// 监听网络端口
	if listener,err = net.Listen("tcp",":"+strconv.Itoa(G_config.ApiPort));err!=nil {
		return
	}

	// 构建http server，设置读写超时时间，以及路由
	httpServer = &http.Server{
		Handler:           mux,
		ReadTimeout:       time.Duration(G_config.ApiReadTimeOut)*time.Millisecond,
		WriteTimeout:      time.Duration(G_config.ApiWriteTimeOut)*time.Millisecond,
	}

	// 初始化单例
	G_apiServer= &ApiServer{
		httpServer:httpServer,
	}

	// 在协程中启动服务
	go httpServer.Serve(listener)



	return


}
