package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/streadway/amqp"
	"mq_tx/models"
	"mq_tx/utils"
	"time"
)

var rabbitMqModel *models.RabbitMqModel
var db *gorm.DB
var rdb *redis.Client
var ctx = context.Background()

// 初始化函数
// 从配置文件中获取密码，连接rabbitMq、mysql和redis
func init()  {
	configModel, err := utils.GetConfigKey()
	if err != nil {
		return
	}

	rabbitMqModel, err = utils.GetRabbitMqModel(configModel)
	if err != nil {
		utils.FailOnError(err, "失败初始化rabbitMq")
		return
	}

	db, err = utils.GetMysqlModel(configModel)
	if err != nil {
		utils.FailOnError(err, "失败初始化mysql")
	}

	rdb, err = utils.GetRedisModel(configModel)
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
	defer utils.CloseRabbitMq(rabbitMqModel)
	defer utils.CloseRedis(rdb)

	db.SingularTable(true)
	db.AutoMigrate(&models.Admin{})

	// 注册消费者
	// 采用手动确认模式，这样自由度比较大
	channel := rabbitMqModel.Channel
	msg, err := channel.Consume(rabbitMqModel.Queue.Name, "", false,
		false, false, false, nil)
	// 接收消息失败，需要重新拉取
	utils.FailOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	// 消费信息
	go consumeMsg(msg)

	<- forever
}

func consumeMsg(msg <-chan amqp.Delivery) {
	for d := range msg {
		fmt.Println(string(d.Body))
		message := new(models.Message)
		err := json.Unmarshal(d.Body, &message)
		if err != nil {
			continue
		}
		// 利用redis进行幂等运算，保证不会重复消费数据
		s := rdb.Get(ctx, message.ID).Val()
		if  s != "" {
			// 相关逻辑处理
			fmt.Println("该消息已经消费过啦")
			// 通知rabbitMq删除该消息
			_ = d.Reject(false)
			continue
		}
		// 消费者执行本地事务
		err = db.Transaction(func(tx *gorm.DB) error {
			// 本地事务的业务逻辑
			err = db.Create(&models.Admin{
				Name:    "test",
				Product: message.Body.Code,
				Address: "sz",
			}).Error
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			// 已经收到了消息，进行事务补偿处理
			fmt.Println("本地事务执行失败")
			continue
		}
		// 本地事务执行成功, 将这条消息的id存入redis中，一小时后自动删除
		rdb.Set(ctx, message.ID, "true", time.Hour)
		// 手动确认
		err = d.Ack(false)
		// 确认失败
		if err != nil {
			// 这部分可以执行重试等策略
			fmt.Println("消息确认失败")
			continue
		}
		fmt.Println("消费者事务执行成功")
	}
}
