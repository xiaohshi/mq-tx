package main

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"mq_tx/models"
	"mq_tx/service"
	"mq_tx/utils"
	"strings"
)

var rabbitMqModel *service.RabbitMqModel
var db *gorm.DB

// 初始化函数
// 从配置文件中获取密码，连接rabbitMq和mysql
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
}

func main()  {
	if rabbitMqModel == nil || db == nil {
		fmt.Println("初始化配置文件出错")
		return
	}

	defer utils.CLoseMysql(db)
	defer rabbitMqModel.Close()

	db.AutoMigrate(&models.Product{})

	// 将这个channel设置为确认模式
	// 放在事务之前设置，如果设置失败整个事务就不会执行
	err := rabbitMqModel.Channel.Confirm(false)
	utils.FailOnError(err, "Failed to set confirm mode")

	// 其中ID定义为事务的版本号，由uuid随机生成
	// 每一个事务都有一个唯一的版本号
	// body是需要发送的数据
	product := models.Product{
		Code: "1111",
		Price: 1000,
	}
	txID := strings.Split(uuid.New().String(), "-")[0]
	msg, err := json.Marshal(models.Message{
		ID:   txID,
		Body: &product,
	})
	utils.FailOnError(err, "Failed")

	// 保证所有异常尽可能都在事务执行之前被发现，保证后续的失败不是程序原因导致
	// 执行本地事务
	err = db.Transaction(func(tx *gorm.DB) error {
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
		// 执行失败策略
		utils.FailOnError(err, "本地事务执行失败")
	}

	// 发送消息
	err, ok := rabbitMqModel.PushMsg(msg)
	if err != nil || !ok {
		// 执行消息发送失败的策略，可以执行回滚操作，将原操作反向执行一次
		fmt.Println("消息发送失败")
	}
}