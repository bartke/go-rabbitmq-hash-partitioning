package common

import (
	"fmt"

	"github.com/streadway/amqp"
)

const (
	hostname = "localhost"
	username = "guest"
	password = "guest"
	scheme   = "amqp"
	port     = 5672

	datafeedExchange = "datafeed"
	registryExchange = "registry"

	registerTopic = "register"
)

func ConnectionString() string {
	return fmt.Sprintf("%s://%s:%s@%s:%d/", scheme, username, password, hostname, port)
}

func SetupRegistryExchange(ch *amqp.Channel) error {
	return setupExchange(ch, registryExchange)
}

func SetupDatafeedExchange(ch *amqp.Channel) error {
	return setupExchange(ch, datafeedExchange)
}

func setupExchange(ch *amqp.Channel, exchange string) error {
	return ch.ExchangeDeclare(
		exchange, // name
		"topic",  // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
}

// SetupQueue creates an exclusive non-durable, autodelete queue
func SetupQueue(ch *amqp.Channel, name string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		name,  // name
		false, // durable
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
}

func BindQueueTopic(ch *amqp.Channel, name, routingKey string) error {
	return bindQueue(ch, name, routingKey, datafeedExchange)
}

func BindRegistry(ch *amqp.Channel, name string) error {
	return bindQueue(ch, name, registerTopic, registryExchange)
}

func bindQueue(ch *amqp.Channel, name, routingKey, exchange string) error {
	return ch.QueueBind(
		name,       // queue name
		routingKey, // routing key
		exchange,   // exchange
		false,
		nil)
}

func Consume(ch *amqp.Channel, queue, consumer string) (<-chan amqp.Delivery, error) {
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return nil, err
	}

	return ch.Consume(
		queue,    // queue
		consumer, // consumer
		true,     // auto-ack (example)
		false,    // exclusive
		false,    // no-local
		false,    // no-wait
		nil,      // args
	)
}

func Heartbeat(ch *amqp.Channel, payload string) error {
	return publish(ch, registryExchange, registerTopic, payload)
}

func Publish(ch *amqp.Channel, routingKey, payload string) error {
	return publish(ch, datafeedExchange, routingKey, payload)
}

func publish(ch *amqp.Channel, exchange, routingKey, payload string) error {
	return ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Transient, // (example)
			ContentType:  "text/plain",
			Body:         []byte(payload),
		})
}
