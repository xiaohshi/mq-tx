package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/gorm"
	"github.com/streadway/amqp"
	"mq_tx/models"
	"time"
)

type RabbitMqModel struct {
	Conn 	*amqp.Connection

	Channel *amqp.Channel

	Queue 	amqp.Queue
}

// 推送消息
// 重试超过10次，就直接返回
func (mqModel *RabbitMqModel)PushMsg(msg []byte) (error, bool) {
	retryCount := 0
	confirms := mqModel.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	for {
		err, ok := mqModel.push(confirms, msg)
		if err != nil || !ok {
			retryCount ++
			if retryCount < 10 {
				continue
			} else {
				return err, false
			}
		}
		fmt.Println("消息发送成功")
		return nil, true
	}
}

// 不带重试的发送消息
func (mqModel *RabbitMqModel)push(confirms chan amqp.Confirmation, msg []byte)(error, bool) {
	// 发送消息
	err := mqModel.Channel.Publish("", mqModel.Queue.Name,
		false, false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType: "text/plain",
			Body:        msg,
		})
	// 如果发送失败返回
	if err != nil {
		return err, false
	}
	// 利用select的特性
	// 一分钟之内没有收到确认消息就返回
	select {
	case confirmed := <-confirms:
		if confirmed.Ack {
			return nil, true
		} else {
			// 在吞吐量过大的时候，rabbit可能会拒绝消息，此时需要回滚事务
			return nil, false
		}
	case <-time.After(time.Minute):
		return nil, false
	}
}

// 消费消息
func (mqModel *RabbitMqModel)ConsumeMsg(msg <-chan amqp.Delivery, rdb *redis.Client, db *gorm.DB) {
	var ctx = context.Background()
	for d := range msg {
		fmt.Println(string(d.Body))
		message := new(models.Message)
		err := json.Unmarshal(d.Body, &message)
		if err != nil {
			continue
		}
		// 利用redis进行幂等运算，保证不会重复消费数据
		if !rdb.SetNX(ctx, message.ID, "true", time.Hour).Val() {
			// 相关逻辑处理
			fmt.Println("该消息已经消费过啦")
			// 通知rabbitMq删除该消息
			_ = d.Reject(false)
			continue
		}
		err = execLocalTx(message, db)
		if err != nil {
			// 已经收到了消息，但是事务执行失败了
			fmt.Println("本地事务执行失败")
			// 启动一个协程执行事务补偿机制
			go compensateTx(message, db)
		}
		// 手动确认
		// 本地事务无论是否执行成功都会确认，保证消息不会堆积
		err = d.Ack(false)
		// 确认失败
		if err != nil {
			fmt.Println("消息确认失败")
			// 重新发送未确认的消息，有幂等保证
			_ = mqModel.Channel.Recover(true)
		}
		fmt.Println("消费者事务执行成功")
	}
}

// 关闭rabbitMq
func (mqModel *RabbitMqModel)Close() {
	if mqModel == nil {
		return
	}
	err := mqModel.Conn.Close()
	if err != nil {
		fmt.Println("连接关闭失败")
	}
	err = mqModel.Channel.Close()
	if err != nil {
		fmt.Println("通道关闭失败")
	}
}

// 事务补偿
// 进入死循环，确保事务一定要执行成功
// 利用事务版本号来判断事务是否被成功执行
func compensateTx(msg *models.Message, db *gorm.DB)  {
	//localMsg := models.LocalMsg{
	//	ID:    msg.ID,
	//	State: false,
	//}
	//db.Create(&localMsg)
	ticker := time.NewTicker(time.Second * 30)
	for range ticker.C {
		err := execLocalTx(msg, db)
		if err == nil {
			//db.Model(&localMsg).Update("state", true)
			return
		}
	}
}

// 消费者执行本地事务
func execLocalTx(msg *models.Message, db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		// 本地事务的业务逻辑
		err := db.Create(&models.Admin{
			Name:    "test",
			Product: msg.Body.Code,
			Address: "sz",
		}).Error
		if err != nil {
			return err
		}
		return nil
	})
}