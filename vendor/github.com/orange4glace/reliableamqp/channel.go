package reliableamqp

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type OnChannelOpened func(*Channel, chan<- bool)
type OnChannelClosed func(*amqp.Error, chan<- bool)

type Channel struct {
	conn *Connection
	*amqp.Channel
	opened                 bool
	notifyM                sync.Mutex
	closeNotify            chan *amqp.Error
	notifyConnectionClosed chan bool
	onOpened               OnChannelOpened
	onClosed               OnChannelClosed
}

func newChannel(conn *Connection, wait int, co OnChannelOpened, cc OnChannelClosed) (c *Channel) {
	c = new(Channel)
	c.conn = conn
	c.notifyConnectionClosed = make(chan bool)
	c.onOpened = co
	c.onClosed = cc
	go func() {
		for {
			log.Printf("Try open channel..")
			err := c.open()
			if err != nil {
				log.Printf("Error %s", err.Error())
			} else {
				// Open event
				log.Printf("Channel is opened")
				c.opened = true
				openCallbackWait := make(chan bool)
				if c.onOpened != nil {
					go c.onOpened(c, openCallbackWait)
				} else {
					close(openCallbackWait)
				}
				// Wait for close
				log.Printf("Channel is waiting for close..")
				internalCloseNotify := make(chan *amqp.Error)
				go func() {
					internalCloseNotify <- <-c.closeNotify
				}()
				<-openCallbackWait
				closeErr := <-c.closeNotify
				// Close event
				c.opened = false
				closeCleanupWait := make(chan bool)
				if c.onClosed != nil {
					go c.onClosed(closeErr, closeCleanupWait)
				} else {
					close(closeCleanupWait)
				}
				<-closeCleanupWait
			}
			time.Sleep(time.Duration(wait) * time.Millisecond)
		}
	}()
	return c
}

func (c *Channel) open() (err error) {
	if c.conn.Connection == nil {
		return errors.New("Connection is closed")
	}
	channel, err := c.conn.Channel()
	if err != nil {
		return err
	}
	c.Channel = channel
	c.closeNotify = channel.NotifyClose(make(chan *amqp.Error))
	return nil
}

func (c *Channel) Cl() {
	c.Close()
}
