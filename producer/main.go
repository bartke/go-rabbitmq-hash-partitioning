package main

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/bartke/go-rabbitmq-partitioned-jobs/common"
	"github.com/streadway/amqp"
)

const (
	consumerTimeout = 600 * time.Millisecond
)

func main() {
	conn, err := amqp.Dial(common.ConnectionString())
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = common.SetupDatafeedExchange(ch)
	failOnError(err, "Failed to declare a exchange")

	registry := NewRegistry(ch)

	// forever
	var counter int
	var routingKey, payload string
	for {
		payload = strconv.Itoa(counter)
		routingKey = common.Hash(counter)

		if registry.ConsumerCount() > 0 {
			fmt.Printf(" [x] Sending %v with route %v\n", payload, routingKey)
			err = common.Publish(ch, routingKey, payload)
			failOnError(err, "Failed to publish a message")
			counter++
		}

		time.Sleep(1000 * time.Millisecond)
	}
	fmt.Println("done.")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type Registry struct {
	ch   *amqp.Channel
	pool sync.Map

	addConsumer    chan string
	deleteConsumer chan string
	consumerCount  int
}

func NewRegistry(ch *amqp.Channel) *Registry {
	q := "register"
	_, err := common.SetupQueue(ch, q)
	failOnError(err, "Failed to declare queue")

	err = common.BindRegistry(ch, q)
	failOnError(err, "Failed to bind queue")

	msgs, err := common.Consume(ch, q, "mgnt")
	failOnError(err, "Failed to register a consumer")

	r := &Registry{
		ch:             ch,
		addConsumer:    make(chan string, 2),
		deleteConsumer: make(chan string, 2),
	}
	go r.runRegistry(msgs)
	go r.runCheckout()
	go r.runBalancer()
	return r
}

func (r *Registry) ConsumerCount() int {
	return r.consumerCount
}

func (r *Registry) runBalancer() {
	for {
		select {
		case k := <-r.addConsumer:
			r.consumerCount++
			fmt.Println("adding consumer to pool:", k)
		case k := <-r.deleteConsumer:
			r.consumerCount--
			fmt.Println("removing consumer from pool:", k)
		}
		err := r.balanceBindings(r.consumerCount)
		if err != nil {
			fmt.Println("Failed to balance bindings", err)
		}
	}
}

func (r *Registry) runCheckout() {
	for {
		time.Sleep(consumerTimeout / 2)
		r.pool.Range(func(ki, vi interface{}) bool {
			k := ki.(string)
			v := vi.(int64)
			lastCheckin := time.Since(time.Unix(0, v))
			if lastCheckin > consumerTimeout {
				r.pool.Delete(k)
				r.deleteConsumer <- k
			}
			return true
		})
	}
}

func (r *Registry) runRegistry(msgs <-chan amqp.Delivery) {
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
	fmt.Println("balancing for", n, "consumers")
	alphabet := len(common.RouteKeys)
	chunks := int(math.Ceil(float64(alphabet) / float64(n)))
	chunki := 0

	type binding struct {
		k, route string
	}
	var bindings []binding
	var unbindings []binding

	r.pool.Range(func(ki, v interface{}) bool {
		k := ki.(string)
		s := chunki * chunks
		e := (chunki + 1) * chunks
		for i := 0; i < alphabet; i++ {
			route := string(common.RouteKeys[i])
			if i >= s && i < e {
				bindings = append(bindings, binding{k: k, route: route})
			} else {
				unbindings = append(unbindings, binding{k: k, route: route})
			}
		}
		chunki++
		return true
	})

	for i := range bindings {
		b := bindings[i]

		// bind route to k
		fmt.Println("binding", b.k, "to", b.route)
		err := common.BindQueueTopic(r.ch, b.k, b.route)
		if err != nil {
			fmt.Println(err)
		}
	}

	for i := range unbindings {
		b := unbindings[i]

		// unbind route from all but k
		fmt.Println("unbinding", b.k, "from", b.route)
		err := common.UnbindQueueTopic(r.ch, b.k, b.route)
		if err != nil {
			fmt.Println(err)
		}
	}

	return nil
}
