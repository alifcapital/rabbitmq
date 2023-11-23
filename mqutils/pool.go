package mqutils

import (
	"errors"
	"sync"

	"github.com/alifcapital/rabbitmq"
)

// Pool is a generic container for keeping rabbitmq clients
type Pool struct {
	container map[string]*rabbitmq.Client
	rw        sync.RWMutex
}

func NewPool() *Pool {
	return &Pool{
		container: make(map[string]*rabbitmq.Client),
		rw:        sync.RWMutex{},
	}
}

func (p *Pool) Register(cfg rabbitmq.ClientConfig) error {
	client, err := rabbitmq.NewClient(cfg)
	if err != nil {
		return err
	}
	p.Set(cfg.DialConfig.AMQPConfig.Vhost, client)
	return nil
}

func (p *Pool) Get(clientName string) (*rabbitmq.Client, bool) {
	client, ok := p.container[clientName]
	return client, ok
}

func (p *Pool) Set(clientName string, client *rabbitmq.Client) {
	p.rw.Lock()
	defer p.rw.Unlock()

	p.container[clientName] = client
}

func (p *Pool) Close() error {
	p.rw.Lock()
	defer p.rw.Unlock()

	var err error
	for clientName, client := range p.container {
		closeErr := client.Close()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
			delete(p.container, clientName)
		}
	}
	return err
}
