package gorp

import "github.com/streadway/amqp"

func SetupExchange(ch *amqp.Channel, exchange, exchangeType string) error {
	return ch.ExchangeDeclare(
		exchange,     // name
		exchangeType, // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
}

// SetupQueue creates an non-durable, autodelete queue
func SetupQueue(ch *amqp.Channel, name string, durable, autodel, exclusive bool) (amqp.Queue, error) {
	return ch.QueueDeclare(
		name,      // name
		durable,   // durable
		autodel,   // delete when unused
		exclusive, // exclusive
		false,     // no-wait
		nil,       // arguments
	)
}

func BindQueueTopic(ch *amqp.Channel, name, routingKey, exchange string) error {
	return ch.QueueBind(
		name,       // queue name
		routingKey, // routing key
		exchange,   // exchange
		false,
		nil)
}

func UnbindQueueTopic(ch *amqp.Channel, name, routingKey, exchange string) error {
	return ch.QueueUnbind(
		name,       // name
		routingKey, // key
		exchange,   // exchange
		nil,        // args
	)
}

func Qos(ch *amqp.Channel, prefetch int) error {
	return ch.Qos(
		prefetch, // prefetch count
		0,        // prefetch size
		false,    // global
	)

}

func Consume(ch *amqp.Channel, queue, consumer string, exclusive bool) (<-chan amqp.Delivery, error) {
	return ch.Consume(
		queue,     // queue
		consumer,  // consumer
		false,     // auto-ack
		exclusive, // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
}

func publish(ch *amqp.Channel, exchange, routingKey string, payload []byte, deliveryMode uint8) error {
	return ch.Publish(
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: deliveryMode,
			ContentType:  "text/plain",
			Body:         payload,
		})
}
