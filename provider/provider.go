package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/streadway/amqp"
	"mq_tx/models"
	"mq_tx/utils"
	"strings"
)

var rabbitMqConfig *models.RabbitMqConfig
var db *gorm.DB

// 初始化函数
// 从配置文件中获取密码，连接rabbitMq和mysql
func init()  {
	configModel, err := utils.GetConfigKey()
	if err != nil {
		return
	}

	rabbitMqConfig, err = utils.GetRabbitMqConfig(configModel)
	if err != nil {
		utils.FailOnError(err, "失败初始化rabbitMq")
		return
	}

	db, err = utils.GetMysqlConfig(configModel)
	if err != nil {
		utils.FailOnError(err, "失败初始化mysql")
	}
}

func main()  {
	if rabbitMqConfig == nil || db == nil {
		fmt.Println("初始化配置文件出错")
		return
	}

	defer utils.CLoseMysql(db)
	defer utils.CloseRabbitMq(rabbitMqConfig)

	db.SingularTable(true)
	// 执行本地事务
	product := models.Product{Code: "1111", Price: 1000}
	err := db.Transaction(func(tx *gorm.DB) error {
		// 插入一行
		err := tx.Create(&product).Error
		if err != nil {
			// 返回任意 err ，整个事务都会 rollback
			return err
		}
		// 返回 nil ，事务会 commit
		return nil
	})
	// 判断本地事务是否执行成功
	if err != nil {
		utils.FailOnError(err, "本地事务执行失败")
	}

	// 本地事务执行成功，发送消息给rabbitmq
	// 将这个通道设置为确认模式
	channel := rabbitMqConfig.Channel
	err = channel.Confirm(false)
	utils.FailOnError(err, "Failed to set confirm mode")

	// 确认消息是否发送成功
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	defer confirmMsg(confirms)

	// 其中ID定义为事务的序列号，由uuid随机生成，body是需要发送的数据
	msg, err := json.Marshal(models.Message{
		ID:   strings.Split(uuid.New().String(), "-")[0],
		Body: &product,
	})
	utils.FailOnError(err, "Failed to publish a message")

	// 发送数据给rabbitMq
	err = channel.Publish("", rabbitMqConfig.Queue.Name, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		})
	utils.FailOnError(err, "Failed to publish a message")
}

// 判断消息是否被推送成功
func confirmMsg(confirms chan amqp.Confirmation)  {
	confirmed := <-confirms
	if confirmed.Ack {
		// 成功的逻辑处理
		fmt.Println("消息发送成功")
	} else {
		// 失败的逻辑处理
		fmt.Println("消息发送失败")
	}
}