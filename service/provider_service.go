package service

import (
	"fmt"
	"github.com/streadway/amqp"
	"mq_tx/models"
	"mq_tx/utils"
	"time"
)

// 推送消息
// 如果发送失败，会直接重新发送一次
// 如果发送成功，可能网络影响，没有收到确认，等待一分钟重新发送一次消息
// 重试超过10次，就直接返回
func PushMsg(mqModel *models.RabbitMqModel, msg []byte) error {
	retryCount := 0
	// 将这个channel设置为确认模式
	channel := mqModel.Channel
	err := channel.Confirm(false)
	utils.FailOnError(err, "Failed to set confirm mode")
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	for {
		// 发送消息
		err = channel.Publish(
			"",
			mqModel.Queue.Name,
			false,
			false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        msg,
			})
		// 发送出现异常，表明肯定没有发送到队列中，直接重试
		if err != nil {
			retryCount ++
			if retryCount <= 10 {
				fmt.Println("发送失败，重新发送")
				continue
			} else {
				return err
			}
		}

		// 利用select的特性
		// 设置定时器，一分钟之内没有收到确认消息就重新发送
		// 消费者要做幂等处理
		ticker := time.NewTicker(time.Minute)
		select {
		case confirmed := <-confirms:
			if confirmed.Ack {
				fmt.Println("消息发送成功")
				return nil
			}
		case <- ticker.C:
		}
		fmt.Println("没有收到确认消息，重新发送")
	}
}