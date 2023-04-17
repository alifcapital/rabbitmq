package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublishNotAckedError struct {
	ExchangeName string
	RoutingKey   string
	Msg          amqp.Publishing
}

func NewErrPublishNotAcked(exName, key string, msg amqp.Publishing) *PublishNotAckedError {
	return &PublishNotAckedError{
		ExchangeName: exName,
		RoutingKey:   key,
		Msg:          msg,
	}
}

func (e *PublishNotAckedError) Error() string {
	return fmt.Sprintf("publish not acknowledged by server, exchange: %s, routing_key: %s", e.ExchangeName, e.RoutingKey)
}
