package prosumer

import (
	"sync"
)

// Prosumer object
type Prosumer struct {
	m              sync.Mutex
	c              *sync.Cond
	q              []interface{}
	closes         []chan bool
	Producer       Producer
	Consumer       Consumer
	closing        bool
	immediate      bool
	closed         bool
	consumerCloser chan bool
}

// NewProsumer create and returns new Prosumer object
// You can access Producer and Consumer via public member
func NewProsumer() (ps *Prosumer) {
	ps = new(Prosumer)
	ps.Producer.ps = ps
	ps.Consumer.ps = ps
	ps.closing = false
	ps.closed = false
	ps.consumerCloser = make(chan bool)
	ps.c = sync.NewCond(&ps.m)
	ps.q = make([]interface{}, 0)

	ps.Consumer.init()
	return ps
}

func (ps *Prosumer) append(any interface{}) {
	ps.q = append(ps.q, any)
}

func (ps *Prosumer) pop() (any interface{}) {
	v := ps.q[0]
	ps.q = ps.q[1:len(ps.q)]
	return v
}

func (ps *Prosumer) len() int {
	return len(ps.q)
}

func (ps *Prosumer) close() {
	ps.m.Lock()
	ps.closed = true
	for _, c := range ps.closes {
		c <- true
	}
	for _, c := range ps.closes {
		close(c)
	}
	ps.m.Unlock()
}

// Close Prosumer
// If immediate is true, Prosumer will be closed immedately.
// Otherwise it will wait until Consumer consumes all remain works
// Producer will be closed immediately in either case.
func (ps *Prosumer) Close(immediate bool) {
	ps.m.Lock()
	ps.closing = true
	ps.immediate = immediate
	ps.c.Broadcast()
	ps.m.Unlock()
	if immediate {
		ps.close()
	} else {
		<-ps.consumerCloser
		ps.close()
	}
}

// NotifyClose returns read-only channel
// The channel gets true value right after Prosumer closed.
func (ps *Prosumer) NotifyClose() <-chan bool {
	ps.m.Lock()
	ch := make(chan bool)
	if ps.closed {
		go func() {
			ch <- true
		}()
		return ch
	}
	ps.closes = append(ps.closes, ch)
	ps.m.Unlock()
	return ch
}
