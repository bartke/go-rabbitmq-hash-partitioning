package common

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
	registerQueue = "register"
	commandQueue  = "command"

	partitionMasterFailover = 2 * time.Second
)

type Registry struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	pool    sync.Map
	timeout time.Duration

	isSlave      bool
	becomeMaster chan bool

	consumerChange time.Time

	wg             sync.WaitGroup
	quitChan       chan struct{}
	addConsumer    chan string
	deleteConsumer chan string
	drain          chan []byte
	consumerCount  int
}

func NewRegistry(conn *amqp.Connection, timeout time.Duration, drain chan []byte) (*Registry, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = SetupRegistryExchange(ch)
	if err != nil {
		return nil, err
	}

	err = SetupCommandExchange(ch)
	if err != nil {
		return nil, err
	}

	r := &Registry{
		conn:           conn,
		ch:             ch,
		timeout:        timeout,
		drain:          drain,
		becomeMaster:   make(chan bool),
		quitChan:       make(chan struct{}),
		addConsumer:    make(chan string, 2),
		deleteConsumer: make(chan string, 2),
	}
	return r, nil
}

// Run tries to acquire the partition master. During it's lifetime a node will
// only become once the master node until it drops out. A slave can only be
// promoted, a master can never be demoted without dropping out.
func (r *Registry) Run() {
	// every instance will be able to execute commands
	go r.runCommandListener()

	t := time.NewTicker(partitionMasterFailover)

	var msgs <-chan amqp.Delivery
	var success bool

loop:
	for {
		r.wg.Add(1)
		select {
		case <-r.quitChan:
			r.wg.Done()
			return
		default:
			msgs, success = r.tryBecomeMaster()
			if success {
				r.wg.Done()
				break loop
			}
			<-t.C
		}
	}
	fmt.Println(" -> assuming partition master")

	go r.runCheckIn(msgs, false)
	go r.runCheckOut(r.timeout, false)
	go r.runBalancer(false)

	if r.isSlave {
		// fire and reset
		close(r.becomeMaster)
		r.becomeMaster = make(chan bool, 1)
		r.isSlave = false
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
	r.wg.Wait()
}

func (r *Registry) createAndAttach(q, t, e string, persistent, exclusive bool) (<-chan amqp.Delivery, *amqp.Channel, error) {
	var err error
	if persistent {
		_, err = SetupQueue(r.ch, q, true, false, false)
	} else {
		_, err = SetupQueue(r.ch, q, false, true, false)
	}
	if err != nil {
		return nil, nil, err
	}

	err = BindQueue(r.ch, q, t, e)
	if err != nil {
		return nil, nil, err
	}

	ch, err := r.conn.Channel()
	if err != nil {
		return nil, ch, err
	}
	msgs, err := Consume(ch, q, "mgnt", exclusive)
	return msgs, ch, err
}

func (r *Registry) tryBecomeMaster() (<-chan amqp.Delivery, bool) {
	msgs, ch, err := r.createAndAttach(
		registerQueue,
		registerTopic,
		registryExchange,
		false, // persistent
		true)  // exclusive
	if err == nil {
		r.wg.Add(1)
		go func() {
			<-r.quitChan
			ch.Close()
			r.wg.Done()
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
		registerQueue+"_slave_"+rand,
		registerTopic,
		registryExchange,
		false, // persistent
		true)  // exclusive
	if err != nil {
		fmt.Println("slave setup failed with consume", err)
		return
	}

	r.isSlave = true
	fmt.Println(" -> assuming partition slave")

	go r.runCheckIn(msgs, true)
	go r.runCheckOut(r.timeout, true)
	go r.runBalancer(true)

	r.wg.Add(1)
	go func() {
		select {
		case <-r.quitChan:
			ch.Close()
			r.wg.Done()
		case <-r.becomeMaster:
			time.Sleep(partitionMasterFailover)
			fmt.Println("detaching slave consumer")
			ch.Close()
			r.wg.Done()
		}
	}()
}

func (r *Registry) runCommandListener() {
	// everyone listens on the same queue, durable for rexecution
	// non-exclusive consumer, round robin between all managers
	msgs, ch, err := r.createAndAttach(
		commandQueue,
		commandTopic,
		commandExchange,
		true,  // persistent
		false) // exclusive
	if err != nil {
		fmt.Println("slave setup failed with consume", err)
		return
	}

	fmt.Println(" -> command manager running")

	r.wg.Add(1)
	go func() {
		for {
			select {
			case <-r.quitChan:
				ch.Close()
				r.wg.Done()
				return
			case c := <-msgs:
				fmt.Printf("command received %s\n", c.Body)
				err, retry := r.routeCommand(string(c.Body))
				if err != nil {
					fmt.Println(err)
					c.Reject(retry)
					continue
				}
				c.Ack(false)
			}
		}
	}()
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
			err := Command(r.ch, commandTopic, fmt.Sprintf("balance:%s:%d", consumerTag, r.consumerCount))
			return fmt.Errorf("retry, consumer count not yet adjusted"), err != nil
		}
		return r.balanceBindings(n), false
	case "retire":
		return r.retireQueue(consumerTag), true
	}
	return nil, false
}

func (r *Registry) runBalancer(isSlave bool) {
	for {
		var k, action string
		select {
		case k = <-r.addConsumer:
			r.consumerCount++
			action = "add"
			fmt.Println(" -> adding consumer to pool:", k)
		case k = <-r.deleteConsumer:
			r.consumerCount--
			action = "delete"
			fmt.Println(" -> removing consumer from pool:", k)
		case <-r.becomeMaster:
			if isSlave {
				fmt.Println("slave exiting balancer")
				return
			}
		}

		r.consumerChange = time.Now()

		// slaves don't command
		if isSlave {
			continue
		}

		err := Command(r.ch, commandTopic, fmt.Sprintf("balance:%s:%d", k, r.consumerCount))
		if err != nil {
			fmt.Println(" [!!] failed to balance bindings for timed out node:", k, err)
		}
		// post rebalancing
		if action == "delete" {
			err = Command(r.ch, commandTopic, "retire:"+k)
			if err != nil {
				fmt.Println(" [!!] failed to retire node:", k, err)
			}

		}
	}
}

func (r *Registry) runCheckOut(timeout time.Duration, isSlave bool) {
	t := time.NewTicker(timeout / 2)
	for {
		select {
		case <-r.becomeMaster:
			if isSlave {
				fmt.Println("slave exiting check-outs")
				return
			}
		case <-t.C:
			r.pool.Range(func(ki, vi interface{}) bool {
				k := ki.(string)
				v := vi.(int64)
				lastCheckIn := time.Since(time.Unix(0, v))
				if lastCheckIn > timeout {
					r.pool.Delete(k)
					r.deleteConsumer <- k
				}
				return true
			})
		}
	}
}

func (r *Registry) runCheckIn(msgs <-chan amqp.Delivery, isSlave bool) {
	for {
		select {
		case <-r.becomeMaster:
			if isSlave {
				fmt.Println("slave exiting check-ins")
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
	fmt.Println(" -> balancing for", n, "consumers")
	alphabet := len(RouteKeys)
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
			route := string(RouteKeys[i])
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
			err = BindQueueTopic(r.ch, b.k, b.route)
		} else {
			fmt.Println("unbinding", b.k, "from", b.route)
			err = UnbindQueueTopic(r.ch, b.k, b.route)
		}
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}

func (r *Registry) retireQueue(k string) error {
	//  1 unbind all
	err := r.unbindAll(k)
	if err != nil {
		return err
	}
	//  2 attach and drain residuals
	q, err := r.ch.QueueInspect(k)
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
	_, err = r.ch.QueueDelete(k, false, false, true)
	return err
}

func (r *Registry) unbindAll(k string) error {
	for i := 0; i < len(RouteKeys); i++ {
		route := string(RouteKeys[i])
		fmt.Println(" -> cleanup unbinding", k, "from", route)
		err := UnbindQueueTopic(r.ch, k, route)
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
	msgs, err := Consume(ch, k, "mgnt", true)
	if err != nil {
		return fmt.Errorf("couldn't consume queue %s", err)
	}
	var i int
	for {
		i++
		d := <-msgs
		d.Ack(false)
		r.drain <- d.Body
		fmt.Println(" -> cleanup drained from ", k, i, "/", m)
		q, _ := r.ch.QueueInspect(k)
		if q.Messages == 0 {
			break
		}
	}
	ch.Close()
	return nil
}
