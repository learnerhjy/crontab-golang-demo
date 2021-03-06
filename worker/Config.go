package worker

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)





// 配置文件
type Config struct {
	EtcdEndPoints []string `json:"etcdEndPoints"`
	EtcdDialTimeOut int `json:"etcdDialTimeOut"`
	MongodbUri string `json:"mongodbUri"`
	MongodbConnectTimeOut int `json:"mongodbConnectTimeOut"`
	JobLogBatchSize int `json:"jobLogBatchSize"`
}

// 单例
var(
	G_config *Config
)
func InitConfig(filename string)(err error){
	var(
		content []byte
		config Config
	)
	// 从文件路径指定路径的文件中读取内容到byte数组中
	if content,err = ioutil.ReadFile(filename);err!=nil{
		return
	}
	// 将byte数组反序列化到对象中
	if err = json.Unmarshal(content,&config);err!=nil{
		return
	}
	// 单例赋值
	G_config = &config

	fmt.Println(G_config)

	return

}
