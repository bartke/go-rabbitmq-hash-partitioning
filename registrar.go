package gorp

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	defaultHeartbeatFrequency = 200 * time.Millisecond
)

type RegistrarConfig struct {
	Tag              string
	RegistryExchange string
	RegisterTopic    string

	HeartbeatFrequency time.Duration
}
type Registrar struct {
	ch     *amqp.Channel
	config RegistrarConfig

	quitConfirmation sync.WaitGroup
	quitChan         chan struct{}
}

func NewRegistrar(conn *amqp.Connection, cfg RegistrarConfig) (*Registrar, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = SetupExchange(ch, cfg.RegistryExchange, "fanout")
	if err != nil {
		return nil, err
	}

	if cfg.HeartbeatFrequency == 0 {
		cfg.HeartbeatFrequency = defaultHeartbeatFrequency
	}

	r := &Registrar{
		ch:       ch,
		config:   cfg,
		quitChan: make(chan struct{}),
	}

	return r, nil
}

// Shutdown blocks until all procedures exit
func (r *Registrar) Shutdown() {
	close(r.quitChan)
	r.quitConfirmation.Wait()
}

func (r *Registrar) Run() {
	t := time.NewTicker(r.config.HeartbeatFrequency)
	r.quitConfirmation.Add(1)
	for {
		select {
		case <-t.C:
			publish(r.ch,
				r.config.RegistryExchange,
				r.config.RegisterTopic,
				[]byte(r.config.Tag),
				amqp.Transient)
		case <-r.quitChan:
			r.quitConfirmation.Done()
			return
		}
	}
}
