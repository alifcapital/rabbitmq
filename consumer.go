package rabbitmq

import (
	"context"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

// An IConsumer handles an amqp message (delivery - in terms of github.com/rabbitmq/amqp091-go library).
//
// Underlying implementations are responsible for either `Acknowledge` or `Reject` of the message.
//
// HINT: one can consume messages the same way as http request handling via http.Handler
// i.e. building chain of middlewares for: panic-handling, logging, tracing and error-handling ...etc.
type IConsumer interface {
	Consume(ctx context.Context, msg amqp.Delivery)
}

type ConsumerFunc func(ctx context.Context, msg amqp.Delivery)

func (f ConsumerFunc) Consume(ctx context.Context, msg amqp.Delivery) {
	f(ctx, msg)
}

// LogConsumer consumer which logs incoming messages
// this is just an example, not aimed for production usage
type LogConsumer struct {
	count uint64
}

func (l *LogConsumer) Consume(ctx context.Context, msg amqp.Delivery) {
	l.count++

	log.Println("New message:", msg.RoutingKey)
	log.Println("Message body:", string(msg.Body))
	log.Println("Total messages received:", l.count)

	if err := msg.Ack(false); err != nil {
		log.Println("Failed acknowledging message, err:", err.Error())
	}
	log.Println("------------------------------------")
}
