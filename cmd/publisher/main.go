package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/alifcapital/rabbitmq/mqutils"
)

func main() {
	client, err := initClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	exchange := "exchange_1"
	routingKey := "event_1"
	body := "hello"

	msg := fmt.Sprintf("%s_%d", body, rand.Int())

	if err := mqutils.Publish(ctx, exchange, routingKey, msg, client); err != nil {
		log.Fatal(err)
	}
}
