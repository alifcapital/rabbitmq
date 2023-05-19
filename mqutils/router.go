package mqutils

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrorHandlerFunc should handle given error and return flag indicating requeue the message or not
// MUST NOT Ack or Nack message
type ErrorHandlerFunc func(ctx context.Context, msg amqp.Delivery, err error) bool

// NotFoundHandlerFunc define "what to do" when given message has no handler specified
// MUST NOT Ack or Nack message
type NotFoundHandlerFunc func(ctx context.Context, msg amqp.Delivery)

// EventHandlerFunc process given msg, errors returned will be handled be provided ErrorHandlerFunc
// MUST NOT Ack or Nack message
type EventHandlerFunc func(ctx context.Context, msg amqp.Delivery) error

// Router implements IConsumer contract
// and takes 2 responsibilities:
// 1. routes messages to their corresponding handlers according to routing keys
// 2. based on processing messages may Ack or Nack them
type Router struct {
	ErrorHandler ErrorHandlerFunc

	NotFoundHandler NotFoundHandlerFunc

	routes map[string]EventHandlerFunc

	mx sync.RWMutex
}

func NewRouter(errorHandler ErrorHandlerFunc, notFoundHandler NotFoundHandlerFunc) *Router {
	return &Router{
		ErrorHandler:    errorHandler,
		NotFoundHandler: notFoundHandler,
		routes:          make(map[string]EventHandlerFunc),
	}
}

func (c *Router) Consume(ctx context.Context, msg amqp.Delivery) {
	eventHandler, ok := c.routes[msg.RoutingKey]
	if !ok {
		c.NotFoundHandler(ctx, msg)

		if err := msg.Nack(false, false); err != nil {
			_ = c.ErrorHandler(ctx, msg, err)
		}

		return
	}

	if err := eventHandler(ctx, msg); err != nil {
		requeue := c.ErrorHandler(ctx, msg, err)

		if err := msg.Nack(false, requeue); err != nil {
			_ = c.ErrorHandler(ctx, msg, err)
		}

		return
	}

	if err := msg.Ack(false); err != nil {
		_ = c.ErrorHandler(ctx, msg, err)
	}
}

func (c *Router) Route(key string, handler EventHandlerFunc) {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.routes[key] = handler
}
