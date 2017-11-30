package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
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
	exitChan := make(chan struct{})
	var wg sync.WaitGroup

	conn, err := amqp.Dial(fmt.Sprintf("%s://%s:%s@%s:%d/", scheme, username, password, hostname, port))
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open channel")
	defer ch.Close()

	// setup queue without binding, durable, non-auto delete, non-exclusive
	_, err = gorp.SetupQueue(ch, tag, true, false, false)
	failOnError(err, "Failed to declare queue")

	gorp.BindQueueTopic(ch, tag, "*a", "test-topic")
	// start exclusive consumer
	msgs, err := gorp.Consume(ch, tag, "comsumer-"+tag, true)
	failOnError(err, "Failed to register a consumer")

	// register and keep alive
	// receive
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		for {
			select {
			case d := <-msgs:
				fmt.Printf(" [<-] %s received: %s via %s\n", tag, d.Body, d.RoutingKey)
				d.Ack(false)
			case <-exitChan:
				defer func(wg *sync.WaitGroup) {
					wg.Done()
				}(wg)
				fmt.Println("shutting down consumer")
				return
			}
		}
	}(&wg)

	// start pinging after we're listening

	fmt.Printf(" [**] %s running\n", tag)
	// blocking wait for SIGTERM
	waitForSigterm()

	fmt.Printf(" [**] %s trying to shutdown..\n", tag)
	close(exitChan)
	wg.Wait()
	ch.Close()
	fmt.Printf(" [**] %s done\n", tag)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Println("%s: %s\n", msg, err)
		os.Exit(1)
	}
}

func waitForSigterm() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	<-c
}
