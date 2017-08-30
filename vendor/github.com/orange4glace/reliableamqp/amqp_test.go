package reliableamqp

import (
	"log"
	"testing"

	"time"

	"strconv"

	"github.com/streadway/amqp"
)

func TestReliableAMQP(t *testing.T) {
	conn := new(Connection)
	conn.OnOpened = testOnOpened
	conn.OnClosed = testOnClosed

	conn.ReliableChannel(testOnChannelOpened, testOnChannelClosed)

	conn.Open("amqp://guest:guest@localhost:5672/", 1000)

	for {

	}
}

func testOnOpened(c *Connection) {
	log.Printf("Connection opened")
}

func testOnClosed(err *amqp.Error) {
	log.Printf("Connection closed. %s", err.Error())
}

func testOnChannelOpened(ch *Channel) {
	log.Printf("Channel opened")
	err := ch.Confirm(false)
	if err != nil {
		log.Printf("Failed to set confirm mode.")
		ch.Cl()
		return
	}
	log.Printf("Channel is now confirm mode.")
	q, err := ch.QueueDeclare(
		"test_queue",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		return
	}
	consumer, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		return
	}
	go func() {
		for delivery := range consumer {
			log.Printf("Delivery : %s", string(delivery.Body))
			delivery.Ack(false)
		}
		log.Printf("Consumer is closed")
	}()

	go func() {
		i := 0
		for {
			ch.Publish("", q.Name, false, false, amqp.Publishing{
				Body: []byte("hello" + strconv.Itoa(i)),
			})
			i++
			time.Sleep(100 * time.Millisecond)
		}
	}()
}

func testOnChannelClosed(err *amqp.Error) {
	log.Printf("Channel closed. %s", err.Error())
}
