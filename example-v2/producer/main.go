package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	gorp "github.com/bartke/go-rabbitmq-hash-partitioning"
	"github.com/streadway/amqp"
)

const (
	hostname = "localhost"
	username = "guest"
	password = "guest"
	scheme   = "amqp"
	port     = 5672

	datafeedExchange = "test-topic"

	consumerTimeout = 600 * time.Millisecond
)

func main() {
	conn, err := amqp.Dial(fmt.Sprintf("%s://%s:%s@%s:%d/", scheme, username, password, hostname, port))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = gorp.SetupExchange(ch, datafeedExchange, "topic")
	failOnError(err, "Failed to declare a exchange")

	// main loop
	var counter int
	go func() {
		for _, routingKey := range []string{"aa", "b", "c", "ac", "ca", "*a"} {
			payload := strconv.Itoa(counter)

			fmt.Printf(" [->] Sending %v with route %v\n", payload, routingKey)
			err = Publish(ch, routingKey, payload)
			failOnError(err, "Failed to publish a message")
			counter++

			time.Sleep(500 * time.Millisecond)
		}
	}()

	waitForSigterm()
	ch.Close()
	fmt.Println("done.")
}

func failOnError(err error, msg string) {
	if err != nil {
		fmt.Printf("%s: %s\n", msg, err)
		os.Exit(1)
	}
}

func waitForSigterm() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	<-c
}

func Publish(ch *amqp.Channel, routingKey, payload string) error {
	return ch.Publish(
		datafeedExchange, // exchange
		routingKey,       // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(payload),
		})
}
