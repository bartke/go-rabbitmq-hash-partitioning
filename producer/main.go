package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/bartke/go-rabbitmq-partitioned-jobs/common"
	"github.com/streadway/amqp"
)

const (
	consumerTimeout = 500 * time.Millisecond
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

	go startRegistry(ch)

	// TODO: wait for consumer

	// forever
	var counter int
	var routingKey, payload string
	for {
		payload = strconv.Itoa(counter)
		routingKey = common.Hash(counter)

		fmt.Printf(" [x] Sending %v with route %v\n", payload, routingKey)
		err = common.Publish(ch, routingKey, payload)
		failOnError(err, "Failed to publish a message")

		time.Sleep(500 * time.Millisecond)

		counter++
		if counter >= 100 {
			break
		}
	}
	fmt.Println("done.")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func startRegistry(ch *amqp.Channel) {
	q, _ := secureRandom(8)
	_, err := common.SetupQueue(ch, q)
	failOnError(err, "Failed to declare queue")

	err = common.BindRegistry(ch, q)
	failOnError(err, "Failed to bind queue")

	msgs, err := common.Consume(ch, q, "mgnt")
	failOnError(err, "Failed to register a consumer")

	pool := make(map[string]int64, 1)
	for d := range msgs {
		t := time.Now().UnixNano()
		c := string(d.Body)
		change := false

		// check in and refresh
		if _, ok := pool[c]; !ok {
			fmt.Println("adding consumer to pool:", c)
			change = true
		}
		pool[c] = t

		// TODO: differrent goroutine

		// check out
		for k, v := range pool {
			if time.Since(time.Unix(0, v)) > consumerTimeout {
				fmt.Println("removing consumer from pool:", k)
				delete(pool, k)
				change = true
			}
		}

		if change {
			balanceBindings(ch, pool)
			failOnError(err, "Failed to balance bindings")
		}
	}
}

func secureRandom(n int) (string, error) {
	bytes := make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func balanceBindings(ch *amqp.Channel, pool map[string]int64) error {
	n := len(pool)
	l := len(common.RouteKeys)
	chunks := int(math.Ceil(float64(l) / float64(n)))
	chunki := 0
	for k := range pool {
		fmt.Println(chunks, "size, binding", chunki*chunks, "-", (chunki+1)*chunks, "to", k)
		for i := chunki * chunks; i < (chunki+1)*chunks && i < l; i++ {
			r := string(common.RouteKeys[i])
			err := common.BindQueueTopic(ch, k, r)
			if err != nil {
				return err
			}
			fmt.Println("binding", k, "to", r)

			// unbind
			for o := range pool {
				if o == k {
					continue
				}
				//fmt.Println("unbinding", o, "from", r)
				err = common.UnbindQueueTopic(ch, o, r)
				if err != nil {
					return err
				}
			}
		}
		chunki++
	}
	return nil
}
