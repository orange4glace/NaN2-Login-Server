package prosumer

import (
	"log"
	"testing"
	"time"
)

func TestProsumer(t *testing.T) {
	ps := NewProsumer()
	p := &ps.Producer
	c := &ps.Consumer

	done := ps.NotifyClose()
	quit := make(chan bool)

	expected := 0

	go func() {
		ch := c.Consume()
		for {
			select {
			case val := <-ch:
				log.Printf("Consume = %d, Expected = %d", val, expected)
				if val != expected {
					t.Errorf("Failed in checking order. Consume = %d, Expected = %d", val, expected)
					t.Fail()
				} else {
					expected++
				}
			case <-done:
				log.Printf("Prosumer is closed.")
				quit <- true
			}

		}
	}()

	go func() {
		for i := 0; i < 50000; i++ {
			p.Produce(i)
			if i == 30000 {
				ps.Close(false)
			}
			time.Sleep(10 * time.Nanosecond)
		}
	}()

	<-quit
	if expected != 30001 {
		t.Errorf("Failed in closing. End with = %d, Expected = %d", expected, 30000)
		t.Fail()
	}
}
