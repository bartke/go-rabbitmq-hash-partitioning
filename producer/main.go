package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bartke/go-rabbitmq-hash-partitioning/common"
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

	drain := make(chan []byte, 16)
	registry, err := common.NewRegistry(conn, consumerTimeout, drain)
	failOnError(err, "Failed to create registry")
	go registry.Run()

	go func() {
		var routingKey, payload string
		for d := range drain {
			payload = string(d)
			routingKey = common.Hash(payload)

			// only refeed if consumer present
			for {
				if registry.SafeToSend() {
					fmt.Printf(" [=>] Sending %v with route %v (drain)\n", payload, routingKey)
					err = common.Publish(ch, routingKey, payload)
					failOnError(err, "Failed to publish a message")
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
		}
	}()

	// forever
	var counter int
	var routingKey, payload string
	for {
		payload = strconv.Itoa(counter)
		routingKey = common.Hash(payload)

		if registry.SafeToSend() {
			fmt.Printf(" [->] Sending %v with route %v\n", payload, routingKey)
			err = common.Publish(ch, routingKey, payload)
			failOnError(err, "Failed to publish a message")
			counter++
		}

		time.Sleep(500 * time.Millisecond)
	}

	ch.Close()
	fmt.Println("done.")
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s\n", msg, err)
		os.Exit(1)
	}
}
