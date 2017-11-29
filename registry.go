package gorp

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	defaultPrefetchCount   = 16
	defaultManagerTimeout  = 2 * time.Second
	defaultConsumerTimeout = 800 * time.Millisecond
)

type Registry struct {
	conn *amqp.Connection
	ch   *amqp.Channel

	pool           sync.Map
	lastExits      *Queue
	consumerChange time.Time

	isSlave      bool
	becomeMaster chan bool

	config RegistryConfig

	addConsumer    chan string
	deleteConsumer chan string
	drain          chan []byte
	consumerCount  int

	quitConfirmation sync.WaitGroup
	quitChan         chan struct{}
}

type RegistryConfig struct {
	RegistryExchange string
	RegisterTopic    string
	RegisterQueue    string

	CommandExchange string
	CommandTopic    string
	CommandQueue    string

	DatafeedExchange string
	Topics           []string

	PrefetchCount   int
	ManagerTimeout  time.Duration
	ConsumerTimeout time.Duration
}

func NewRegistry(conn *amqp.Connection, cfg RegistryConfig, drain chan []byte) (*Registry, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = SetupExchange(ch, cfg.RegistryExchange, "fanout")
	if err != nil {
		return nil, err
	}

	err = SetupExchange(ch, cfg.CommandExchange, "fanout")
	if err != nil {
		return nil, err
	}

	if cfg.PrefetchCount == 0 {
		cfg.PrefetchCount = defaultPrefetchCount
	}

	if cfg.ManagerTimeout == 0 {
		cfg.ManagerTimeout = defaultManagerTimeout
	}

	if cfg.ConsumerTimeout == 0 {
		cfg.ConsumerTimeout = defaultConsumerTimeout
	}

	if cfg.PrefetchCount == 0 {
		cfg.PrefetchCount = 1
	}

	r := &Registry{
		conn:           conn,
		ch:             ch,
		drain:          drain,
		becomeMaster:   make(chan bool),
		quitChan:       make(chan struct{}),
		addConsumer:    make(chan string, 2),
		deleteConsumer: make(chan string, 2),
		lastExits:      NewQueue(3),
		config:         cfg,
	}
	return r, nil
}

// Run tries to acquire the partition master. During it's lifetime a node will
// only become once the master node until it drops out. A slave can only be
// promoted, a master can never be demoted without dropping out.
func (r *Registry) Run() {
	t := time.NewTicker(r.config.ManagerTimeout)

	var msgs <-chan amqp.Delivery
	var success bool

	r.quitConfirmation.Add(1)
loop:
	for {
		select {
		case <-r.quitChan:
			r.quitConfirmation.Done()
			return
		default:
			msgs, success = r.tryBecomeMaster()
			if success {
				r.quitConfirmation.Done()
				break loop
			}
			<-t.C
		}
	}
	fmt.Println(" -> assuming partition master")

	go r.runCheckIn(msgs, false)
	go r.runCheckOut(false)
	go r.runBalancer(false)

	if r.isSlave {
		// fire and reset
		close(r.becomeMaster)
		r.becomeMaster = make(chan bool, 1)
		r.isSlave = false
	}

	// only master runs the command listener
	go r.runCommandListener()

	fmt.Println(" -> master assumed, rebalance and checking recently exited consumers")

	err := r.checkRecentExits()
	if err != nil {
		fmt.Println(err)
	}
}

func (r *Registry) ConsumerStatus() (int, time.Time) {
	return r.consumerCount, r.consumerChange
}

func (r *Registry) SafeToSend() bool {
	cnt, chg := r.ConsumerStatus()
	if cnt > 0 && time.Since(chg) > time.Second {
		return true
	}
	return false
}

// Shutdown blocks until all procedures exit
func (r *Registry) Shutdown() {
	close(r.quitChan)
	r.quitConfirmation.Wait()
}

func (r *Registry) createAndAttach(q, t, e string, persistent, exclusive bool) (<-chan amqp.Delivery, *amqp.Channel, error) {
	var err error
	_, err = SetupQueue(r.ch, q, persistent, !persistent, false)
	if err != nil {
		return nil, nil, err
	}

	err = BindQueueTopic(r.ch, q, t, e)
	if err != nil {
		return nil, nil, err
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return nil, ch, err
	}
	err = Qos(ch, r.config.PrefetchCount)
	if err != nil {
		return nil, ch, fmt.Errorf("couldn't set qos %s", err)
	}
	msgs, err := Consume(ch, q, "mgnt", exclusive)
	return msgs, ch, err
}

func (r *Registry) tryBecomeMaster() (<-chan amqp.Delivery, bool) {
	msgs, ch, err := r.createAndAttach(
		r.config.RegisterQueue,
		r.config.RegisterTopic,
		r.config.RegistryExchange,
		false, // persistent
		true)  // exclusive
	if err == nil {
		r.quitConfirmation.Add(1)
		go func() {
			defer ch.Close()
			defer r.quitConfirmation.Done()
			<-r.quitChan
		}()
		return msgs, true
	}

	if !r.isSlave {
		r.setupSlave()
	}
	return nil, false
}

func (r *Registry) setupSlave() {
	rand, _ := secureRandom(5)
	msgs, ch, err := r.createAndAttach(
		r.config.RegisterQueue+"_slave_"+rand,
		r.config.RegisterTopic,
		r.config.RegistryExchange,
		false, // persistent
		true)  // exclusive
	if err != nil {
		fmt.Println("slave setup failed with consume", err)
		return
	}

	r.isSlave = true
	fmt.Println(" -> assuming partition slave")

	go r.runCheckIn(msgs, true)
	go r.runCheckOut(true)
	go r.runBalancer(true)

	r.quitConfirmation.Add(1)
	go func() {
		defer ch.Close()
		defer r.quitConfirmation.Done()
		// blocking wait
		select {
		case <-r.quitChan:
		case <-r.becomeMaster:
			time.Sleep(r.config.ManagerTimeout)
		}
	}()
}

func (r *Registry) runCommandListener() {
	// durable for rexecution, exclusive for order
	msgs, ch, err := r.createAndAttach(
		r.config.CommandQueue,
		r.config.CommandTopic,
		r.config.CommandExchange,
		true, // persistent
		true) // exclusive
	if err != nil {
		fmt.Println("slave setup failed with consume", err)
		return
	}

	fmt.Println(" -> command manager running")

	r.quitConfirmation.Add(1)
	go func() {
		defer ch.Close()
		defer r.quitConfirmation.Done()
		var retries int
		for {
			// one at a time
			select {
			case <-r.quitChan:
				return
			case c := <-msgs:
				fmt.Printf("command received %s\n", c.Body)
				err, retry := r.routeCommand(string(c.Body))
				if err != nil {
					retries++
					fmt.Println(err)
					c.Reject(retry && retries < 2)
					if retries >= 2 {
						retries = 0
					}
					continue
				}
				c.Ack(false)
			}
		}
	}()
}

func (r *Registry) Command(payload []byte) error {
	return publish(r.ch, r.config.CommandExchange, r.config.CommandTopic, payload, amqp.Persistent)
}

func (r *Registry) routeCommand(cmd string) (error, bool) {
	params := strings.Split(cmd, ":")
	lp := len(params)
	if lp < 2 || lp > 3 {
		return fmt.Errorf(" [!!] unknown command received %s", cmd), false
	}
	action := params[0]
	consumerTag := params[1]
	switch action {
	case "balance":
		// don't crash here
		if lp != 3 {
			return fmt.Errorf("wrong balance command received"), false
		}
		n, err := strconv.Atoi(params[2])
		if err != nil {
			return err, false
		}
		if n != r.consumerCount {
			payload := []byte(fmt.Sprintf("balance:%s:%d", consumerTag, r.consumerCount))
			err := r.Command(payload)
			return fmt.Errorf("retry, consumer count not yet adjusted"), err != nil
		}
		return r.balanceBindings(n), false
	case "retire":
		return r.retireQueue(consumerTag), true
	}
	return nil, false
}

func (r *Registry) runBalancer(isSlave bool) {
	r.quitConfirmation.Add(1)
	defer r.quitConfirmation.Done()
	for {
		var k, action string
		select {
		case <-r.quitChan:
			return
		case k = <-r.addConsumer:
			r.consumerCount++
			action = "add"
			fmt.Println(" -> adding consumer to pool:", k)
		case k = <-r.deleteConsumer:
			r.consumerCount--
			action = "delete"
			r.lastExits.Push(k)
			fmt.Println(" -> removing consumer from pool:", k)
		case <-r.becomeMaster:
			if isSlave {
				return
			}
		}

		r.consumerChange = time.Now()

		// slaves don't command
		if isSlave {
			continue
		}

		payload := []byte(fmt.Sprintf("balance:%s:%d", k, r.consumerCount))
		err := r.Command(payload)
		if err != nil {
			fmt.Println(" [!!] failed to balance bindings for timed out node:", k, err)
		}
		// post rebalancing
		if action == "delete" {
			err = r.Command([]byte("retire:" + k))
			if err != nil {
				fmt.Println(" [!!] failed to retire node:", k, err)
			}

		}
	}
}

func (r *Registry) runCheckOut(isSlave bool) {
	t := time.NewTicker(r.config.ConsumerTimeout / 2)
	r.quitConfirmation.Add(1)
	defer r.quitConfirmation.Done()
	for {
		select {
		case <-r.quitChan:
			return
		case <-r.becomeMaster:
			if isSlave {
				return
			}
		case <-t.C:
			r.pool.Range(func(ki, vi interface{}) bool {
				k := ki.(string)
				v := vi.(int64)
				lastCheckIn := time.Since(time.Unix(0, v))
				if lastCheckIn > r.config.ConsumerTimeout {
					r.pool.Delete(k)
					r.deleteConsumer <- k
				}
				return true
			})
		}
	}
}

func (r *Registry) runCheckIn(msgs <-chan amqp.Delivery, isSlave bool) {
	r.quitConfirmation.Add(1)
	defer r.quitConfirmation.Done()
	for {
		select {
		case <-r.quitChan:
			return
		case <-r.becomeMaster:
			if isSlave {
				return
			}
		case d := <-msgs:
			d.Ack(false)
			t := time.Now().UnixNano()
			k := string(d.Body)

			// check in and refresh
			if _, ok := r.pool.LoadOrStore(k, t); !ok {
				r.addConsumer <- k
				continue
			}
			r.pool.Store(k, t)
		}
	}
}

func (r *Registry) balanceBindings(n int) error {
	//if n == 0 {
	//	return fmt.Errorf("no consumers")
	//}
	fmt.Println(" -> balancing for", n, "consumers")
	alphabet := len(r.config.Topics)
	chunks := int(math.Ceil(float64(alphabet) / float64(n)))
	chunki := 0

	type binding struct {
		k, route, t string
	}
	var bindings []binding

	r.pool.Range(func(ki, v interface{}) bool {
		k := ki.(string)
		s := chunki * chunks
		e := (chunki + 1) * chunks
		for i := 0; i < alphabet; i++ {
			route := r.config.Topics[i]
			if i >= s && i < e {
				bindings = append(bindings, binding{k: k, route: route, t: "bind"})
			} else {
				bindings = append(bindings, binding{k: k, route: route, t: "unbind"})
			}
		}
		chunki++
		return true
	})

	for i := range bindings {
		b := bindings[i]

		var err error
		if b.t == "bind" {
			fmt.Println("binding", b.k, "to", b.route)
			err = BindQueueTopic(r.ch, b.k, b.route, r.config.DatafeedExchange)
		} else {
			fmt.Println("unbinding", b.k, "from", b.route)
			err = UnbindQueueTopic(r.ch, b.k, b.route, r.config.DatafeedExchange)
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Registry) retireQueue(k string) error {
	// this one may break, get a new channel on every try
	ch, err := r.conn.Channel()
	if err != nil {
		// if this fails we're screwed anyways
		return fmt.Errorf("couldn't open channel %s", err)
	}
	defer ch.Close()

	//  1 unbind all
	err = r.unbindAll(k)
	if err != nil {
		return err
	}
	//  2 attach and drain residuals
	q, err := ch.QueueInspect(k)
	if err != nil {
		return err
	}
	if q.Messages > 0 {
		fmt.Println(" -> cleanup draining", q.Messages, "messages")
		err = r.drainQueue(k, q.Messages)
		if err != nil {
			return err
		}
	}
	//  3 delete queue
	fmt.Println(" -> cleanup deleting queue", k)
	// agressive delete here, may need to retry above in production setup
	_, err = ch.QueueDelete(k, false, false, true)
	return err
}

func (r *Registry) unbindAll(k string) error {
	for i := 0; i < len(r.config.Topics); i++ {
		route := r.config.Topics[i]
		fmt.Println(" -> cleanup unbinding", k, "from", route)
		err := UnbindQueueTopic(r.ch, k, route, r.config.DatafeedExchange)
		if err != nil {
			return fmt.Errorf("unbinding %s from %s failed with %s", k, route, err)
		}
	}
	return nil
}

func (r *Registry) drainQueue(k string, m int) error {
	ch, err := r.conn.Channel()
	if err != nil {
		return fmt.Errorf("couldn't open channel %s", err)
	}
	err = Qos(ch, 1)
	if err != nil {
		return fmt.Errorf("couldn't set qos %s", err)
	}
	msgs, err := Consume(ch, k, "mgnt", true)
	if err != nil {
		return fmt.Errorf("couldn't consume queue %s", err)
	}
	for i := m; i > 0; i-- {
		d := <-msgs
		r.drain <- d.Body
		fmt.Printf(" => cleanup drain %s from %s\n", d.Body, k)
		d.Ack(false)
	}
	fmt.Println(" -> cleanup", k, "drained.")
	ch.Close()
	return nil
}

func (r *Registry) checkRecentExits() error {
	// rebalance first so we don't loose anything while retiring
	err := r.balanceBindings(r.consumerCount)
	if err != nil {
		fmt.Println(err)
	}

	var configChange bool
	for i := 0; i < r.lastExits.Len(); i++ {
		k := r.lastExits.Pop()
		// this one may break, get a new channel on every try
		ch, err := r.conn.Channel()
		if err != nil {
			// if this fails we're screwed anyways
			return fmt.Errorf("couldn't open channel %s", err)
		}
		q, err := ch.QueueInspect(k)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if q.Consumers == 0 {
			fmt.Println(" XX retiring stuck queue", k)
			err = r.retireQueue(k)
			if err != nil {
				fmt.Println(err)
			}
			configChange = true
		}
		ch.Close()
	}
	if configChange {
		fmt.Println(" XX rebalancing after config change ")
		return r.balanceBindings(r.consumerCount)
	}
	return nil
}
