package main

import (
	"fmt"
	"time"

	"github.com/alifcapital/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func initClient() (*rabbitmq.Client, error) {
	return rabbitmq.NewClient(
		rabbitmq.ClientConfig{
			NetworkErrCallback: func(a *amqp.Error) {
				fmt.Println("NetworkErrCallback:", a.Error())
			},

			AutoRecoveryInterval: time.Second * 5,

			AutoRecoveryErrCallback: func(err error) bool {
				fmt.Println("AutoRecoveryErrCallback:", err.Error())
				return true
			},

			ConsumerAutoRecoveryErrCallback: func(consumer rabbitmq.AMQPConsumer, err error) {
				fmt.Println("ConsumerAutoRecoveryErrCallback:", err.Error())
			},

			DialConfig: rabbitmq.DialConfig{
				User:     "",
				Password: "",
				Host:     "",
				Port:     "",
				AMQPConfig: amqp.Config{
					Vhost: "/",
				},
			},
			PublisherConfirmEnabled: true,
			PublisherConfirmNowait:  false,
			ConsumerQos:             0,
			ConsumerPrefetchSize:    0,
			ConsumerGlobal:          false,
		},
	)
}
