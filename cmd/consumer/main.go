package main

import (
	"context"
	"log"
	"os"

	"github.com/alifcapital/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	client, err := initClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := client.Consume(rabbitmq.AMQPConsumer{
		ExchangeParams: rabbitmq.ExchangeParams{
			Name:       "exchange_1",
			Type:       amqp.ExchangeTopic,
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			Nowait:     false,
			Args: amqp.Table{
				"x-queue-type":     "quorum",
				"x-delivery-limit": 3,
			},
			DeclareExchange: true,
		},
		QueueParams: rabbitmq.QueueParams{
			Name:       "queue_1",
			Durable:    true,
			AutoDelete: false,
			Exclusive:  false,
			Nowait:     false,
			Args:       nil,
		},
		QueueBindParams: rabbitmq.QueueBindParams{},
		ConsumerParams: rabbitmq.ConsumerParams{
			RoutingKeys: []string{"event_1"},
			ConsumerID:  "consumer_1",
			AutoAck:     false,
			Exclusive:   false,
			NoLocal:     false,
			Nowait:      false,
			Args:        nil,
		},
		IConsumer: rabbitmq.ConsumerFunc(func(ctx context.Context, msg amqp.Delivery) {
			defer msg.Ack(false)

			log.Printf("Consumer: 1, exchange: %s, key: %s, msg: %s\n", msg.Exchange, msg.RoutingKey, string(msg.Body))
		}),
	}); err != nil {
		log.Fatal(err)
	}

	ch := make(chan os.Signal)
	switch <-ch {
	case os.Interrupt:
		log.Println("Bye")
		break
	}
}
