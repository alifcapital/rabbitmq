// Package rabbitmq - all structs like SmthParams are wrappers for amqp091-go library function arguments
// for documentation and usage examples see:
// - https://github.com/rabbitmq/amqp091-go
// - https://www.rabbitmq.com/documentation.html
package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

type ExchangeParams struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	Nowait     bool
	Args       amqp.Table
	// custom flag
	DeclareExchange bool
}

type QueueParams struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Nowait     bool
	Args       amqp.Table
}

type QueueBindParams struct {
	Nowait bool
	Args   amqp.Table
}

type ConsumerParams struct {
	RoutingKeys []string
	ConsumerID  string
	AutoAck     bool
	Exclusive   bool
	NoLocal     bool
	Nowait      bool
	Args        amqp.Table
}

type AMQPConsumer struct {
	ExchangeParams
	QueueParams
	QueueBindParams
	ConsumerParams

	IConsumer
}
