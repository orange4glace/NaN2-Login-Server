package prosumer

// Consumer object
type Consumer struct {
	ps *Prosumer
	ch chan interface{}
}

func (c *Consumer) init() {
	c.ch = make(chan interface{})
	go func() {
		for {
			c.ps.m.Lock()
			if c.ps.closed {
				return
			}
			if c.ps.len() == 0 {
				if c.closeCheck() {
					c.ps.m.Unlock()
					return
				}
				c.ps.c.Wait()
				if c.closeCheck() {
					c.ps.m.Unlock()
					return
				}
			}
			v := c.ps.pop()
			c.ps.m.Unlock()
			c.ch <- v
		}
	}()
}

// Consume returns a read-only consumer channel
func (c *Consumer) Consume() <-chan interface{} {
	return c.ch
}

func (c *Consumer) closeCheck() bool {
	if !c.ps.closing {
		return false
	}
	if !c.ps.immediate {
		c.ps.consumerCloser <- true
	}
	return true
}
