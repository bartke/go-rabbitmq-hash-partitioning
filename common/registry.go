package common

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Registry struct {
	ch   *amqp.Channel
	pool sync.Map

	addConsumer    chan string
	deleteConsumer chan string
	consumerCount  int
}

func NewRegistry(ch *amqp.Channel, timeout time.Duration) (*Registry, error) {
	q := "register"
	_, err := SetupQueue(ch, q)
	if err != nil {
		return nil, err
	}

	err = BindRegistry(ch, q)
	if err != nil {
		return nil, err
	}

	msgs, err := Consume(ch, q, "mgnt")
	if err != nil {
		return nil, err
	}

	r := &Registry{
		ch:             ch,
		addConsumer:    make(chan string, 2),
		deleteConsumer: make(chan string, 2),
	}
	go r.runCheckin(msgs)
	go r.runCheckout(timeout)
	go r.runBalancer()
	return r, nil
}

func (r *Registry) ConsumerCount() int {
	return r.consumerCount
}

func (r *Registry) runBalancer() {
	for {
		select {
		case k := <-r.addConsumer:
			r.consumerCount++
			fmt.Println(" -> adding consumer to pool:", k)
		case k := <-r.deleteConsumer:
			r.consumerCount--
			fmt.Println(" -> removing consumer from pool:", k)
		}
		err := r.balanceBindings(r.consumerCount)
		if err != nil {
			fmt.Println(" !! failed to balance bindings:", err)
		}
	}
}

func (r *Registry) runCheckout(timeout time.Duration) {
	for {
		time.Sleep(timeout / 2)
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

func (r *Registry) runCheckin(msgs <-chan amqp.Delivery) {
	for d := range msgs {
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
			//fmt.Println("binding", b.k, "to", b.route)
			err = BindQueueTopic(r.ch, b.k, b.route)
		} else {
			//fmt.Println("unbinding", b.k, "from", b.route)
			err = UnbindQueueTopic(r.ch, b.k, b.route)
		}
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}
