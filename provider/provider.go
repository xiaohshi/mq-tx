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

var rabbitMqModel *models.RabbitMqModel
var db *gorm.DB

// 初始化函数
// 从配置文件中获取密码，连接rabbitMq和mysql
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
}

func main()  {
	if rabbitMqModel == nil || db == nil {
		fmt.Println("初始化配置文件出错")
		return
	}

	defer utils.CLoseMysql(db)
	defer utils.CloseRabbitMq(rabbitMqModel)

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
		// 执行失败策略
		utils.FailOnError(err, "本地事务执行失败")
	}

	// 其中ID定义为事务的版本号，由uuid随机生成，每一个事务只有一个版本号
	// body是需要发送的数据
	msg, err := json.Marshal(models.Message{
		ID:   strings.Split(uuid.New().String(), "-")[0],
		Body: &product,
	})
	utils.FailOnError(err, "Failed")

	// 发送消息
	err = service.PushMsg(rabbitMqModel, msg)
	if err != nil {
		fmt.Println("消息发送失败")
	}
}