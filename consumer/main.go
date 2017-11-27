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
	exitChan := make(chan struct{})
	var wg sync.WaitGroup

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

	// register and keep alive
	// receive
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
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
	}(&wg)

	// start pinging after we're listening

	wg.Add(1)
	go heartbeat(ch, tag, &wg, exitChan)

	fmt.Printf(" [**] %s running\n", tag)
	// blocking wait for SIGTERM
	waitForSigterm()

	fmt.Printf(" [**] %s trying to shutdown..\n", tag)
	// after SIGTERM trigger exit and wait for all procedures to finish
	close(exitChan)
	wg.Wait()
	// detach
	ch.Close()
	fmt.Printf(" [**] %s graceful shutdown\n", tag)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Println("%s: %s\n", msg, err)
		os.Exit(1)
	}
}

func heartbeat(ch *amqp.Channel, consumer string, wg *sync.WaitGroup, exitChan chan struct{}) {
	t := time.NewTicker(heartbeatFrequency)
	for {
		select {
		case <-t.C:
			err := common.Heartbeat(ch, consumer)
			failOnError(err, "Failed to publish a message")
		case <-exitChan:
			fmt.Println("shutting down heartbeats")
			wg.Done()
			return
		}
	}
}

func waitForSigterm() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM)
	signal.Notify(c, syscall.SIGINT)
	<-c
}
