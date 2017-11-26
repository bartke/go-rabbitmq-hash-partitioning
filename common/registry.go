package common

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	registerQueue           = "register"
	partitionMasterFailover = 2 * time.Second
)

type Registry struct {
	conn    *amqp.Connection
	ch      *amqp.Channel
	pool    sync.Map
	timeout time.Duration

	isSlave      bool
	becomeMaster chan bool

	addConsumer    chan string
	deleteConsumer chan string
	consumerCount  int
}

func NewRegistry(conn *amqp.Connection, timeout time.Duration) (*Registry, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	r := &Registry{
		conn:           conn,
		ch:             ch,
		timeout:        timeout,
		becomeMaster:   make(chan bool, 1),
		addConsumer:    make(chan string, 2),
		deleteConsumer: make(chan string, 2),
	}
	return r, nil
}

// Run tries to acquire the partition master. During it's lifetime a node will
// only become once the master node until it drops out. A slave can only be
// promoted, a master can never be demoted without dropping out.
func (r *Registry) Run() {
	var msgs <-chan amqp.Delivery
	for {
		_, err := SetupQueue(r.ch, registerQueue, false, true, false)
		if err != nil {
			fmt.Println("couldn't setup queue", err)
		}

		err = BindRegistry(r.ch, registerQueue)
		if err != nil {
			fmt.Println("couldn't bind queue", err)
		}

		ch, err := r.conn.Channel()
		if err != nil {
			fmt.Println("couldn't open channel", err)
		}
		msgs, err = Consume(ch, registerQueue, "mgnt")
		if err == nil {
			break
		}
		if !r.isSlave {
			r.setupSlave()
		}
		time.Sleep(partitionMasterFailover)
	}
	fmt.Println(" -> assuming partition master")

	go r.runCheckin(msgs, false)
	go r.runCheckout(r.timeout, false)
	go r.runBalancer(false)

	if r.isSlave {
		// fire and reset
		close(r.becomeMaster)
		r.becomeMaster = make(chan bool, 1)
		r.isSlave = false
	}
}

func (r *Registry) ConsumerCount() int {
	return r.consumerCount
}

func (r *Registry) setupSlave() {
	rand, _ := secureRandom(5)
	q := registerQueue + "_slave_" + rand
	_, err := SetupQueue(r.ch, q, false, true, false)
	if err != nil {
		fmt.Println("slave setup failed with queue", err)
		return
	}

	err = BindRegistry(r.ch, q)
	if err != nil {
		fmt.Println("slave setup failed with bind", err)
		return
	}

	ch, err := r.conn.Channel()
	if err != nil {
		fmt.Println("couldn't open channel", err)
		return
	}

	msgs, err := Consume(ch, q, "mgnt")
	if err != nil {
		fmt.Println("slave setup failed with consume", err)
		return
	}

	r.isSlave = true
	fmt.Println(" -> assuming partition slave")

	go r.runCheckin(msgs, true)
	go r.runCheckout(r.timeout, true)
	go r.runBalancer(true)

	go func() {
		select {
		case <-r.becomeMaster:
			time.Sleep(partitionMasterFailover)
			fmt.Println("detaching slave consumer")
			ch.Close()
		}
	}()
}

func (r *Registry) runBalancer(isSlave bool) {
	for {
		select {
		case k := <-r.addConsumer:
			r.consumerCount++
			fmt.Println(" -> adding consumer to pool:", k)
		case k := <-r.deleteConsumer:
			r.consumerCount--
			fmt.Println(" -> removing consumer from pool:", k)
		case <-r.becomeMaster:
			if isSlave {
				fmt.Println("slave exiting balancer")
				return
			}
		}
		if isSlave {
			// slaves don't balance
			continue
		}
		err := r.balanceBindings(r.consumerCount)
		if err != nil {
			fmt.Println(" !! failed to balance bindings:", err)
		}
	}
}

func (r *Registry) runCheckout(timeout time.Duration, isSlave bool) {
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
				lastCheckin := time.Since(time.Unix(0, v))
				if lastCheckin > timeout {
					r.pool.Delete(k)
					r.deleteConsumer <- k
				}
				return true
			})
		}
	}
}

func (r *Registry) runCheckin(msgs <-chan amqp.Delivery, isSlave bool) {
	for {
		select {
		case <-r.becomeMaster:
			if isSlave {
				fmt.Println("slave exiting check-ins")
				return
			}
		case d := <-msgs:
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
	if n == 0 {
		return fmt.Errorf("no consumers")
	}
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
