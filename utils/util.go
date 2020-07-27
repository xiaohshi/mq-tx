package utils

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/gorm"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"mq_tx/models"
)

// 失败报错
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", err, msg)
	}
}

// 通过配置文件得到user、password和host
func GetConfigKey()(*models.ConfigKey, error){
	config, err := ioutil.ReadFile("D:/config.yaml")
	if err != nil {
		return nil, err
	}
	configModel := new(models.ConfigKey)
	err = yaml.Unmarshal(config, configModel)
	if err != nil {
		return nil, err
	}
	return configModel, nil
}

// 获取rabbitMq的配置
func GetRabbitMqConfig(configModel *models.ConfigKey)(*models.RabbitMqConfig, error){
	data, ok := configModel.Def["rabbitmq"]
	if !ok {
		return nil, errors.New("获取不到rabbitMq配置")
	}
	url := fmt.Sprintf("amqp://%s:%s@%s/", data.User, data.Password, data.Host)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	queue, err := channel.QueueDeclare("test", true, false,
		false, false, nil)
	if err != nil {
		return nil, err
	}
	return &models.RabbitMqConfig{
		Conn:    conn,
		Channel: channel,
		Queue:   queue,
	}, nil
}

// 获取mysql的配置
func GetMysqlConfig(configModel *models.ConfigKey)(*gorm.DB, error) {
	data, ok := configModel.Def["mysql"]
	if !ok {
		return nil, errors.New("获取不到mysql配置")
	}
	url := fmt.Sprintf("%s:%s@(%s)/test?charset=utf8&parseTime=True&loc=Local",
		data.User, data.Password, data.Host)
	return gorm.Open("mysql", url)
}

// 获取redis配置
func GetRedisConfig(configModel *models.ConfigKey)(*redis.Client, error) {
	data, ok := configModel.Def["redis"]
	if !ok {
		return nil, errors.New("获取不到redis配置")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     data.Host,
		Password: data.Password,
		DB:       0,  // use default DB
	})
	return rdb, nil
}

// 关闭rabbitMq
func CloseRabbitMq(mqConfig *models.RabbitMqConfig) {
	if mqConfig == nil {
		return
	}
	mqConfig.Conn.Close()
	mqConfig.Channel.Close()
}

// 关闭mysql
func CLoseMysql(db *gorm.DB) {
	if db == nil {
		return
	}
	db.Close()
}

func CloseRedis(rdb *redis.Client)  {
	if rdb == nil {
		return
	}
	rdb.Close()
}
