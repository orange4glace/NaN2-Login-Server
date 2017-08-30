package prosumer

// Producer object
type Producer struct {
	ps *Prosumer
}

func newProducer(prosumer *Prosumer) (p *Producer) {
	p = new(Producer)
	p.ps = prosumer
	return p
}

// Produce an data of any type (interface{})
// Returns true when success
// Returns false when failed (Prosumer is closed)
func (p *Producer) Produce(data interface{}) bool {
	p.ps.m.Lock()
	defer p.ps.m.Unlock()
	if p.ps.closing {
		return false
	}
	p.ps.append(data)
	p.ps.c.Broadcast()
	return true
}
