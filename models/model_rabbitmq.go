package models

import "github.com/streadway/amqp"

type RabbitMqModel struct {
	Conn 	*amqp.Connection

	Channel *amqp.Channel

	Queue 	amqp.Queue
}