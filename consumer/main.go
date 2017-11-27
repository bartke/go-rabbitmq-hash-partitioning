package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/bartke/go-rabbitmq-partitioned-jobs/common"
	"github.com/streadway/amqp"
)

const (
	heartbeatFrequency = 200 * time.Millisecond
)

var tag string

func init() {
	const (
		defaultTag = "c1"
		usage      = "Use a specific consumer tag used for its subscription."
	)
	flag.StringVar(&tag, "tag", defaultTag, usage)
}

func main() {
	flag.Parse()

	conn, err := amqp.Dial(common.ConnectionString())
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	// ensure the registry exists
	err = common.SetupRegistryExchange(ch)
	failOnError(err, "Failed to declare a exchange")

	// setup queue without binding
	_, err = common.SetupQueue(ch, tag, true, false, false)
	failOnError(err, "Failed to declare queue")

	// start consumer
	msgs, err := common.Consume(ch, tag, "comsumer-"+tag)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	// register and keep alive
	// receive
	go func() {
		for d := range msgs {
			fmt.Printf(" [<-] %s received: %s via %s\n", tag, d.Body, d.RoutingKey)
			d.Ack(false)
		}
	}()

	// start pinging after we're listening
	go heartbeat(ch, tag)

	fmt.Printf(" [**] %s running\n", tag)
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Println("%s: %s\n", msg, err)
		os.Exit(1)
	}
}

func heartbeat(ch *amqp.Channel, consumer string) {
	for {
		err := common.Heartbeat(ch, consumer)
		failOnError(err, "Failed to publish a message")
		time.Sleep(heartbeatFrequency)
	}
}
