package mqutils

import (
	"context"
	"errors"

	"github.com/alifcapital/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler is a function should be some kind of controller.Method
// where you can handle a kind of business logic or call underlying function
// and return error which will be handled by underlying ErrorHandler
type Handler func(ctx context.Context, msg amqp.Delivery) error

// ErrorHandler should handle error during Handler phase and return indicator of `requeue`
// i.e. should message be redelivered again
type ErrorHandler func(ctx context.Context, msg amqp.Delivery, err error) bool

type Router struct {
	errorHandler      ErrorHandler
	eventConsumers    map[string]rabbitmq.IConsumer
	globalMiddlewares []ConsumerMiddleware
}

func NewRouter(errorHandler ErrorHandler, mids ...ConsumerMiddleware) *Router {
	return &Router{
		errorHandler:      errorHandler,
		eventConsumers:    make(map[string]rabbitmq.IConsumer),
		globalMiddlewares: append([]ConsumerMiddleware{}, mids...),
	}
}

func (r *Router) Consume(ctx context.Context, msg amqp.Delivery) {
	eventConsumer, ok := r.eventConsumers[msg.RoutingKey]
	if !ok {
		err := NewHandlerNotFoundError(msg)

		// when handler for a routing key is not found, retrying it makes no sense
		_ = r.errorHandler(ctx, msg, err)

		if err := msg.Nack(false, false); err != nil {
			nackErr := errors.Join(NewNackFailedError(msg), err)
			_ = r.errorHandler(ctx, msg, nackErr)
		}

		return
	}

	eventConsumer.Consume(ctx, msg)
}

func (r *Router) RegisterEventHandler(eventName string, handler Handler, middlewares ...ConsumerMiddleware) {
	mids := append(r.globalMiddlewares, middlewares...)

	consumer := combineConsumerMiddlewares(
		r.makeConsumer(handler),
		mids...,
	)

	r.eventConsumers[eventName] = consumer
}

func (r *Router) GetEventNames() []string {
	var names []string
	for k := range r.eventConsumers {
		names = append(names, k)
	}
	return names
}

func (r *Router) makeConsumer(handler Handler) rabbitmq.IConsumer {
	return rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
		if err := handler(ctx, msg); err != nil {
			// handle error
			requeue := r.errorHandler(ctx, msg, err)

			// try sending NOT-ACKNOWLEDGED (fail)
			if err := msg.Nack(false, requeue); err != nil {
				nackErr := errors.Join(NewNackFailedError(msg), err)
				_ = r.errorHandler(ctx, msg, nackErr)
			}
		} else {
			// try sending ACKNOWLEDGED (success)
			if err := msg.Ack(false); err != nil {
				nackErr := errors.Join(NewAckFailedError(msg), err)
				_ = r.errorHandler(ctx, msg, nackErr)
			}
		}
	})
}

func combineConsumerMiddlewares(consumer rabbitmq.IConsumer, mids ...ConsumerMiddleware) rabbitmq.IConsumer {
	midsLen := len(mids)
	if midsLen == 0 {
		return consumer
	}
	firstChain := consumer
	for i := midsLen; i > 0; i-- {
		firstChain = mids[i-1](firstChain)
	}
	return firstChain
}
