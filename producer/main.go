package main

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"strconv"
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

	go startRegistry(ch)

	// TODO: wait for consumer

	// forever
	var counter int
	var routingKey, payload string
	for {
		payload = strconv.Itoa(counter)
		routingKey = hash(counter)

		err = common.Publish(ch, routingKey, payload)
		failOnError(err, "Failed to publish a message")

		log.Printf(" [x] Sent %v with route %v", payload, routingKey)
		time.Sleep(500 * time.Millisecond)

		counter++
		if counter >= 100 {
			break
		}
	}
	log.Println("done.")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func hash(input int) string {
	return string(byte(input%26 + 97))
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
		confChange := false
		t := time.Now().UnixNano()
		c := string(d.Body)

		// check in and refresh
		if _, ok := pool[c]; !ok {
			log.Println("adding consumer to pool:", c)
			confChange = true
		}
		pool[c] = t

		// TODO: differrent goroutine
		// check out
		for k, v := range pool {
			if time.Since(time.Unix(0, v)) > consumerTimeout {
				log.Println("removing consumer from pool:", k)
				confChange = true
				delete(pool, k)
			}
		}

		// balance bindings
		if confChange {
			n := len(pool)
			l := len(common.RouteKeys)
			chunks := l / n
			chunki := 0
			for k, _ := range pool {
				//log.Println("binding", chunki*chunks, "-", (chunki+1)*chunks, "to", k)
				for i := chunki * chunks; i < (chunki+1)*chunks && i < l; i++ {
					r := string(common.RouteKeys[i])
					err := common.BindQueueTopic(ch, k, r)
					failOnError(err, "Failed to bind queue")
					//log.Println("binding", k, "to", r)

					// unbind
					for o, _ := range pool {
						if o == k {
							continue
						}
						//log.Println("unbinding", o, "from", r)
						err = common.UnbindQueueTopic(ch, o, r)
						failOnError(err, "Failed to unbind queue")
					}
				}
				chunki++
			}
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
