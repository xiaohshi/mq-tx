package service

import (
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/jinzhu/gorm"
	"github.com/streadway/amqp"
	"mq_tx/models"
)

type ConfigKey struct {
	Def 	 map[string]*models.ConfigData `yaml:"def"`
}

// 获取rabbitMq的配置
func (configModel *ConfigKey)GetRabbitMqModel()(*RabbitMqModel, error){
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
	return &RabbitMqModel{
		Conn:    conn,
		Channel: channel,
		Queue:   queue,
	}, nil
}

// 获取mysql的配置
func (configModel *ConfigKey)GetMysqlModel()(*gorm.DB, error) {
	data, ok := configModel.Def["mysql"]
	if !ok {
		return nil, errors.New("获取不到mysql配置")
	}
	url := fmt.Sprintf("%s:%s@(%s)/test?charset=utf8&parseTime=True&loc=Local",
		data.User, data.Password, data.Host)
	db, err := gorm.Open("mysql", url)
	if err != nil {
		return nil, err
	}
	db.SingularTable(true)
	db.DB().SetMaxOpenConns(10)
	db.DB().SetMaxIdleConns(5)
	return db, nil
}

// 获取redis配置
func (configModel *ConfigKey)GetRedisModel()(*redis.Client, error) {
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