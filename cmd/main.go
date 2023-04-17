package main

import (
	"context"
	"log"
	"time"

	"github.com/alifcapital/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	client, err := rabbitmq.NewClient(rabbitmq.ClientConfig{
		NetworkErrCallback: func(a *amqp.Error) {
			log.Println("NetworkErrCallback:", a.Error())
		},
		AutoRecoveryInterval: time.Second * 10,
		AutoRecoveryErrCallback: func(err error) bool {
			log.Println("AutoRecoveryErrCallback:", err.Error())
			return true
		},
		ConsumerAutoRecoveryErrCallback: func(consumer rabbitmq.AMQPConsumer, err error) {
			log.Println("ConsumerAutoRecoveryErrCallback:", err.Error())
			log.Println("ConsumerAutoRecoveryErrCallback.consumer:", consumer.ConsumerID)
		},
		DialConfig: rabbitmq.DialConfig{
			User:     "rabbitmq",
			Password: "secret",
			Host:     "localhost",
			Port:     "5672",
			AMQPConfig: amqp.Config{
				Vhost: "/",
			},
		},
		PublisherConfirmEnabled: true,
		PublisherConfirmNowait:  false,
		ConsumerQos:             2,
		ConsumerPrefetchSize:    0,
		ConsumerGlobal:          false,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := registerConsumers(client); err != nil {
		log.Fatal(err)
	}

	log.Println("sending message 1 with in confirm mode...")
	if err := client.PublishConfirm(
		context.Background(),
		"main",
		"topic_1",
		false,
		false,
		amqp.Publishing{
			Headers:         nil,
			ContentType:     "",
			ContentEncoding: "",
			DeliveryMode:    0,
			Priority:        0,
			CorrelationId:   "",
			ReplyTo:         "",
			Expiration:      "",
			MessageId:       "",
			Timestamp:       time.Time{},
			Type:            "",
			UserId:          "",
			AppId:           "",
			Body:            []byte(`123`),
		},
	); err != nil {
		log.Println("failed on publishing message 1")
	}
	log.Println("finished sending message 1")

	log.Println("Will wait for 30 second for you to kill some rabbitmq node")
	time.Sleep(time.Second * 30)

	log.Println("sending message 2 without confirm ...")
	if err := client.Publish(
		context.Background(),
		"main",
		"topic_2",
		false,
		false,
		amqp.Publishing{
			Headers:         nil,
			ContentType:     "",
			ContentEncoding: "",
			DeliveryMode:    0,
			Priority:        0,
			CorrelationId:   "",
			ReplyTo:         "",
			Expiration:      "",
			MessageId:       "",
			Timestamp:       time.Time{},
			Type:            "",
			UserId:          "",
			AppId:           "",
			Body:            []byte(`321`),
		},
	); err != nil {
		log.Println("failed on publishing message 2")
	}
	log.Println("finished sending message 2")

	time.Sleep(time.Second * 5)
	log.Println("buy")
}
