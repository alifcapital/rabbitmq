package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type DialConfig struct {
	User       string
	Password   string
	Host       string
	Port       string
	AMQPConfig amqp.Config
}

// Dial a handy wrapper for base "github.com/rabbitmq/amqp091-go" DialConfig function
func Dial(cfg DialConfig) (*amqp.Connection, error) {
	proto := "amqp"
	if cfg.AMQPConfig.TLSClientConfig != nil {
		proto = "amqps"
	}
	url := fmt.Sprintf("%s://%s:%s@%s:%s/", proto, cfg.User, cfg.Password, cfg.Host, cfg.Port)
	return amqp.DialConfig(url, cfg.AMQPConfig)
}
