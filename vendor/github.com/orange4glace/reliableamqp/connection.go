package reliableamqp

import (
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	reconnectInterval = 10
)

type OnConnectionOpened func(*Connection)
type OnConnectionClosed func(*amqp.Error)

type Connection struct {
	*amqp.Connection
	ReconnectInterval int
	closeNotify       chan *amqp.Error
	OnOpened          OnConnectionOpened
	OnClosed          OnConnectionClosed
	channelLock       sync.Mutex
	opened            bool
	channels          []*Channel
	channelWG         sync.WaitGroup
}

func NewConnection() *Connection {
	c := new(Connection)
	c.channels = make([]*Channel, 0)
	return c
}

func (c *Connection) Open(url string, wait int) {
	c.Connection, _ = amqp.Dial(url)
	go func() {
		for {
			err := c.open(url)
			if err != nil {
				time.Sleep(time.Duration(wait) * time.Millisecond)
			} else {
				// Connection opened
				c.opened = true
				if c.OnOpened != nil {
					go c.OnOpened(c)
				}
				// Wait for close
				closeErr := <-c.closeNotify
				// Connection closed
				c.opened = false
				if c.OnClosed != nil {
					go c.OnClosed(closeErr)
				}
			}
		}
	}()
}

func (c *Connection) open(url string) (err error) {
	log.Printf("# Try to open..")
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	c.Connection = conn
	c.opened = true
	c.closeNotify = conn.NotifyClose(make(chan *amqp.Error))
	return nil
}

func (c *Connection) Cl() {
	c.Close()
}

func (c *Connection) ReliableChannel(co OnChannelOpened, cc OnChannelClosed) (ch *Channel) {
	c.channelLock.Lock()
	defer c.channelLock.Unlock()
	ch = newChannel(c, 1000, co, cc)
	if ch == nil {
		// Connection is closed
		return nil
	}
	c.channels = append(c.channels, ch)
	return ch
}
