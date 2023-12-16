package mqutils

import (
	"errors"
	"strings"
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

func (p *Pool) Register(cfg rabbitmq.ClientConfig) (*rabbitmq.Client, error) {
	var b strings.Builder
	b.WriteString(cfg.DialConfig.AMQPConfig.Vhost)
	b.WriteString(cfg.DialConfig.User)
	b.WriteString(cfg.DialConfig.Port)
	b.WriteString(cfg.DialConfig.Host)
	clientName := b.String()

	client, exists := p.Get(clientName)
	if exists {
		return client, nil
	}

	client, err := rabbitmq.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	p.Set(clientName, client)
	return client, nil
}

func (p *Pool) Get(clientName string) (*rabbitmq.Client, bool) {
	p.rw.Lock()
	defer p.rw.Unlock()

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
