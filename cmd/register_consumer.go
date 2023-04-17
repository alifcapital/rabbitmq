package main

import (
	"context"
	"log"

	"github.com/alifcapital/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func registerConsumers(client *rabbitmq.Client) error {
	deadLetterExchange := "dead_letter"
	deadLetterConsumer := MakeConsumer(
		deadLetterExchange,
		amqp.ExchangeTopic,
		"dead_letter_consumer",
		amqp.Table{
			"x-queue-type":     "quorum",
			"x-delivery-limit": 3,
		},
		[]string{"*"},
		nil,
	)

	mainConsumer := MakeConsumer(
		"main",
		amqp.ExchangeTopic,
		"main_consumer_1",
		amqp.Table{
			"x-queue-type":           "quorum",
			"x-delivery-limit":       3,
			"x-dead-letter-exchange": deadLetterExchange,
		},
		[]string{"topic_1", "topic_2"},
		rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			log.Println("main_consumer_1, body:", string(msg.Body))
			if err := msg.Ack(false); err != nil {
				log.Println("failed to ack on mainConsumer")
			}
		}),
	)

	secondaryConsumer := MakeConsumer(
		"main",
		amqp.ExchangeTopic,
		"main_consumer_2",
		amqp.Table{
			"x-queue-type":           "quorum",
			"x-delivery-limit":       3,
			"x-dead-letter-exchange": deadLetterExchange,
		},
		[]string{"topic_2", "topic_3"},
		rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			log.Println("main_consumer_2, body:", string(msg.Body))
			if err := msg.Ack(false); err != nil {
				log.Println("failed to ack on mainConsumer")
			}
		}),
	)

	var err error
	err = client.Consume(deadLetterConsumer)
	err = client.Consume(mainConsumer)
	err = client.Consume(secondaryConsumer)
	return err
}
