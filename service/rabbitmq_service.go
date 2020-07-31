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

var ctx = context.Background()

type RabbitMqModel struct {
	Conn 	*amqp.Connection

	Channel *amqp.Channel

	Queue 	amqp.Queue
}

// 推送消息
// 重试超过5次，就直接返回
func (mqModel *RabbitMqModel)PushMsg(msg []byte) (error, bool) {
	retryCount := 0
	confirms := mqModel.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	for {
		err, ok := mqModel.push(confirms, msg)
		if err != nil || !ok {
			retryCount ++
			if retryCount < 5 {
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
		fmt.Println("发送失败，重试")
		return err, false
	}
	// 利用select的特性
	// 一分钟之内没有收到确认消息就返回
	// 时间不能设置太短，不然会出现一致性问题
	select {
	case confirmed := <-confirms:
		if confirmed.Ack {
			return nil, true
		} else {
			fmt.Println("消息被拒绝，重试")
			// 在吞吐量过大的时候，rabbit可能会拒绝消息，重试
			// 可能需要在该过程中执行一些相关策略，不一定是重试
			return nil, false
		}
	case <-time.After(time.Second * 10):
		fmt.Println("确认失败，重试")
		return nil, false
	}
}

// 消费消息
// 事务执行和确认分开执行，事务执行失败会不断重试直至成功
// 在考虑是否应该将消息存入本地的消息表中
func (mqModel *RabbitMqModel)ConsumeMsg(msg <-chan amqp.Delivery, rdb *redis.Client, db *gorm.DB) {
	for d := range msg {
		fmt.Println(string(d.Body))
		message := new(models.Message)
		err := json.Unmarshal(d.Body, &message)
		if err != nil {
			fmt.Println("该消息可能有错误，删除")
			// 通知rabbitMq删除该消息
			_ = d.Reject(false)
			continue
		}
		go mqModel.handle(d, rdb, db, message)
	}
}

// 关闭rabbitMq
func (mqModel *RabbitMqModel)Close() {
	if mqModel == nil {
		return
	}
	err := mqModel.Channel.Close()
	if err != nil {
		fmt.Println("通道关闭失败")
	}
	err = mqModel.Conn.Close()
	if err != nil {
		fmt.Println("连接关闭失败")
	}

}

// 本地事务处理
func (mqModel *RabbitMqModel)handle(d amqp.Delivery, rdb *redis.Client, db *gorm.DB, msg *models.Message)  {
	// 利用redis进行幂等运算，保证不会重复消费数据
	if rdb.Get(ctx, msg.ID).Val() != "" {
		// 相关逻辑处理
		fmt.Println("该消息已经消费过啦")
		// 通知rabbitMq删除该消息
		_ = d.Reject(false)
		return
	}
	err := db.Transaction(func(tx *gorm.DB) error {
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
	if err != nil {
		fmt.Println("事务消费失败")
		// 重试
		_ = mqModel.Channel.Recover(true)
		return
	}
	// 将事务ID存入redis, 1小时后删除
	rdb.SetNX(ctx, msg.ID, "true", time.Hour)

	// 手动确认消息
	err = d.Ack(false)
	// 确认失败
	if err != nil {
		fmt.Println("消息确认失败")
		// 重新发送未确认的消息，有幂等保证
		_ = mqModel.Channel.Recover(true)
		return
	}
	fmt.Println("消费者事务执行成功")
}

// 事务补偿
// 进入死循环，确保事务一定要执行成功
// 利用事务版本号来判断事务是否被成功执行
func compensateTx(msg *models.Message, db *gorm.DB)  {
	localMsg := models.LocalMsg{
		ID:    msg.ID,
		State: false,
	}
	db.Create(&localMsg)
	ticker := time.NewTicker(time.Second * 30)
	for range ticker.C {
		//execLocalTx(msg, db)
		//if err == nil {
		//	//db.Model(&localMsg).Update("state", true)
		//	return
		//}
	}
}

// 消费者执行本地事务
func execLocalTx(msg *models.Message, db *gorm.DB) {
	for {
		err := db.Transaction(func(tx *gorm.DB) error {
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
		if err == nil {
			return
		}
		// 已经收到了消息，但是事务执行失败了
		fmt.Println("本地事务执行失败, 重试中")
	}
}