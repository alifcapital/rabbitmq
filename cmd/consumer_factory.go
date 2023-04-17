package main

import (
	"github.com/alifcapital/rabbitmq"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

func MakeConsumer(
	exchangeName, exchangeType string,
	queueName string, queueArgs amqp.Table,
	routingKeys []string,
	consumer rabbitmq.IConsumer,
) rabbitmq.AMQPConsumer {
	return rabbitmq.AMQPConsumer{
		ExchangeParams: rabbitmq.ExchangeParams{
			Name:       exchangeName,
			Type:       exchangeType,
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			Nowait:     false,
			Args:       nil,
		},
		QueueParams: rabbitmq.QueueParams{
			Name:       queueName,
			Durable:    true,
			AutoDelete: false,
			Exclusive:  false,
			Nowait:     false,
			Args:       queueArgs,
		},
		QueueBindParams: rabbitmq.QueueBindParams{
			Nowait: false,
			Args:   nil,
		},
		ConsumerParams: rabbitmq.ConsumerParams{
			RoutingKeys: routingKeys,
			ConsumerID:  uuid.NewString(),
			AutoAck:     false,
			Exclusive:   false,
			NoLocal:     false,
			Nowait:      false,
			Args:        nil,
		},
		IConsumer: consumer,
	}
}
