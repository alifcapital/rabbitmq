package rabbitmq

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ClientConfig struct {
	// this callback will be invoked whenever network failure happens or node shuts down
	NetworkErrCallback func(*amqp.Error)
	// note that this interval is taken into account when on reconnecting multiple times in row
	AutoRecoveryInterval time.Duration
	// this callback will be invoked whenever connection retry fails or resource freeing returns error
	// returning value as bool should indicate whether to keep retrying recovery or not
	AutoRecoveryErrCallback func(error) bool
	// this callback will be called if restoring a consumer fails after recovery procedure
	ConsumerAutoRecoveryErrCallback func(AMQPConsumer, error)

	// configurations for setting up dial and new connection
	DialConfig

	PublisherConfirmEnabled bool
	PublisherConfirmNowait  bool

	ConsumerQos          int
	ConsumerPrefetchSize int
	ConsumerGlobal       bool
}

type Client struct {
	cfg           ClientConfig
	connection    *amqp.Connection
	publisherChan *amqp.Channel
	consumerChan  *amqp.Channel

	consumers []AMQPConsumer

	mx sync.RWMutex
	wg sync.WaitGroup
}

func NewClient(cfg ClientConfig) (*Client, error) {
	client := Client{cfg: cfg}
	if err := client.connect(); err != nil {
		return nil, err
	}

	return &client, nil
}

func (c *Client) connect() error {
	conn, err := Dial(c.cfg.DialConfig)
	if err != nil {
		return err
	}

	// it's recommended to separate publisher and consumer channels in order to avoid heavy control-flows
	// https://www.rabbitmq.com/channels.html#flow-control
	publisherChannel, err := conn.Channel()
	if err != nil {
		return err
	}
	if c.cfg.PublisherConfirmEnabled {
		if err := publisherChannel.Confirm(c.cfg.PublisherConfirmNowait); err != nil {
			return err
		}
	}

	// create new channel for consumer
	consumerChannel, err := conn.Channel()
	if err != nil {
		return err
	}
	if err := consumerChannel.Qos(
		c.cfg.ConsumerQos,
		c.cfg.ConsumerPrefetchSize,
		c.cfg.ConsumerGlobal,
	); err != nil {
		return err
	}

	// reassign
	c.connection = conn
	c.publisherChan = publisherChannel
	c.consumerChan = consumerChannel

	go func() {
		errNotifyChan := c.connection.NotifyClose(make(chan *amqp.Error))

		for connectionErr := range errNotifyChan {
			c.cfg.NetworkErrCallback(connectionErr)

			c.reconnect()
		}
	}()

	return nil
}

func (c *Client) reconnect() {
	if err := c.Close(); err != nil {
		shouldContinue := c.cfg.AutoRecoveryErrCallback(err)
		if !shouldContinue {
			return
		}
	}

	// try to connect
	if err := c.connect(); err != nil {
		shouldContinue := c.cfg.AutoRecoveryErrCallback(err)
		if shouldContinue {
			// when instant reconnect fails, try again after some time
			time.AfterFunc(c.cfg.AutoRecoveryInterval, func() {
				c.reconnect()
			})
		}
	} else {
		// connection was successful, so restore consumers
		for _, consumerParam := range c.consumers {
			if err := c.consume(consumerParam); err != nil {
				c.cfg.ConsumerAutoRecoveryErrCallback(consumerParam, err)
			}
		}
	}
}

func (c *Client) Consume(consumer AMQPConsumer) error {
	c.mx.Lock()
	defer c.mx.Unlock()

	c.consumers = append(c.consumers, consumer)

	return c.consume(consumer)
}

func (c *Client) consume(consumer AMQPConsumer) error {
	if err := c.consumerChan.ExchangeDeclare(
		consumer.ExchangeParams.Name,
		consumer.ExchangeParams.Type,
		consumer.ExchangeParams.Durable,
		consumer.ExchangeParams.AutoDelete,
		consumer.ExchangeParams.Internal,
		consumer.ExchangeParams.Nowait,
		consumer.ExchangeParams.Args,
	); err != nil {
		return err
	}

	queue, err := c.consumerChan.QueueDeclare(
		consumer.QueueParams.Name,
		consumer.QueueParams.Durable,
		consumer.QueueParams.AutoDelete,
		consumer.QueueParams.Exclusive,
		consumer.QueueParams.Nowait,
		consumer.QueueParams.Args,
	)
	if err != nil {
		return err
	}

	for _, key := range consumer.RoutingKeys {
		// The default exchange is a `direct type` with no name (empty string) pre-declared by the broker.
		// It has one special property that makes it very useful for simple applications:
		// every queue that is created is automatically bound to it with a routing event which is the same as the queue name.
		if consumer.ExchangeParams.Name != "" {
			if err := c.consumerChan.QueueBind(
				queue.Name,
				key,
				consumer.ExchangeParams.Name,
				consumer.QueueBindParams.Nowait,
				consumer.QueueBindParams.Args,
			); err != nil {
				return err
			}
		}
	}

	if consumer.IConsumer != nil {
		deliveries, err := c.consumerChan.Consume(
			consumer.QueueParams.Name,
			consumer.ConsumerParams.ConsumerID,
			consumer.ConsumerParams.AutoAck,
			consumer.ConsumerParams.Exclusive,
			consumer.ConsumerParams.NoLocal,
			consumer.ConsumerParams.Nowait,
			consumer.ConsumerParams.Args,
		)
		if err != nil {
			return err
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()

			for msg := range deliveries {
				consumer.Consume(context.Background(), msg)
			}
		}()
	}

	return nil
}

func (c *Client) Publish(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.publisherChan.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (c *Client) PublishConfirm(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	defConfirm, err := c.publisherChan.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil {
		return err
	}
	success := defConfirm.Wait()
	if !success {
		return NewErrPublishNotAcked(exchange, key, msg)
	}
	return nil
}

func (c *Client) Close() error {
	var err error
	if !c.connection.IsClosed() {
		err = c.publisherChan.Close()

		for _, consumer := range c.consumers {
			err = c.consumerChan.Cancel(consumer.ConsumerID, false)
		}
		c.wg.Wait()

		err = c.consumerChan.Close()

		err = c.connection.Close()
	}

	return err
}
