package models

import "github.com/streadway/amqp"

type RabbitMqConfig struct {
	Conn 	*amqp.Connection

	Channel *amqp.Channel

	Queue 	amqp.Queue
}