package mqutils

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type HandlerNotFoundError struct {
	msg amqp.Delivery
}

type NackFailedError struct {
	msg amqp.Delivery
}

type AckFailedError struct {
	msg amqp.Delivery
}

func NewHandlerNotFoundError(msg amqp.Delivery) *HandlerNotFoundError {
	return &HandlerNotFoundError{msg: msg}
}

func NewNackFailedError(msg amqp.Delivery) *NackFailedError {
	return &NackFailedError{msg: msg}
}

func NewAckFailedError(msg amqp.Delivery) *AckFailedError {
	return &AckFailedError{msg: msg}
}

func (err *HandlerNotFoundError) Error() string {
	return fmt.Sprintf("handler not found for routing_key: %s", err.msg.RoutingKey)
}

func (err *NackFailedError) Error() string {
	return fmt.Sprintf("NACK failed for routing_key: %s", err.msg.RoutingKey)
}

func (err *AckFailedError) Error() string {
	return fmt.Sprintf("ACK failed for routing_key: %s", err.msg.RoutingKey)
}
