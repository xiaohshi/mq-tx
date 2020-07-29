package main

import (
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"mq_tx/models"
	"mq_tx/service"
	"mq_tx/utils"
)

var rabbitMqModel *service.RabbitMqModel
var db *gorm.DB
var rdb *redis.Client

// 初始化函数
// 从配置文件中获取密码，连接rabbitMq、mysql和redis
func init()  {
	configModel, err := utils.GetConfigKey()
	if err != nil {
		return
	}

	rabbitMqModel, err = configModel.GetRabbitMqModel()
	if err != nil {
		utils.FailOnError(err, "失败初始化rabbitMq")
		return
	}

	db, err = configModel.GetMysqlModel()
	if err != nil {
		utils.FailOnError(err, "失败初始化mysql")
	}

	rdb, err = configModel.GetRedisModel()
	if err != nil {
		utils.FailOnError(err, "失败初始化redis")
	}
}

func main()  {
	if rabbitMqModel == nil || db == nil || rdb == nil {
		fmt.Println("初始化配置文件出错")
		return
	}

	defer utils.CLoseMysql(db)
	defer rabbitMqModel.Close()
	defer utils.CloseRedis(rdb)

	db.AutoMigrate(&models.Admin{})

	// 注册消费者, 采用手动确认模式
	msg, err := rabbitMqModel.Channel.Consume(rabbitMqModel.Queue.Name, "", false,
		false, false, false, nil)
	utils.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	// 消费信息
	go rabbitMqModel.ConsumeMsg(msg, rdb, db)

	<- forever
}
