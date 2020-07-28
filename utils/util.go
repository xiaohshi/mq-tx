package utils

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/gorm"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"mq_tx/service"
)

// 失败报错
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", err, msg)
	}
}

// 通过配置文件得到user、password和host
func GetConfigKey()(*service.ConfigKey, error){
	config, err := ioutil.ReadFile("D:/config.yaml")
	if err != nil {
		return nil, err
	}
	configModel := new(service.ConfigKey)
	err = yaml.Unmarshal(config, configModel)
	if err != nil {
		return nil, err
	}
	return configModel, nil
}

// 关闭mysql
func CLoseMysql(db *gorm.DB) {
	if db == nil {
		return
	}
	err := db.Close()
	if err != nil {
		fmt.Println("mysql关闭失败")
	}
}

// 关闭redis
func CloseRedis(rdb *redis.Client)  {
	if rdb == nil {
		return
	}
	err := rdb.Close()
	if err != nil {
		fmt.Println("redis关闭失败")
	}
}
