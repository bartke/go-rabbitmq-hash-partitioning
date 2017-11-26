package main

import (
	"fmt"
	"os"
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

	err = common.SetupRegistryExchange(ch)
	failOnError(err, "Failed to declare a exchange")

	registry, err := common.NewRegistry(conn, consumerTimeout)
	failOnError(err, "Failed to create registry")
	go registry.Run()

	// forever
	var counter int
	var routingKey, payload string
	for {
		payload = strconv.Itoa(counter)
		routingKey = common.Hash(counter)

		if registry.ConsumerCount() > 0 {
			fmt.Printf(" [->] Sending %v with route %v\n", payload, routingKey)
			err = common.Publish(ch, routingKey, payload)
			failOnError(err, "Failed to publish a message")
			counter++
		}

		time.Sleep(500 * time.Millisecond)
	}
	fmt.Println("done.")
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s\n", msg, err)
		os.Exit(1)
	}
}
