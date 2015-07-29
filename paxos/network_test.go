package paxos

import (
	"container/heap"
	"log"
	"math/rand"
)

// http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.21.5841&rep=rep1&type=pdf
type link struct {
	drop float64

	min    float64
	stddev float64
	mean   float64
}

func (l link) transfer() (delay float64, ok bool) {
	if rand.Float64() < l.drop {
		return 0, false
	}
	for i := 0; i < 100; i++ {
		d := rand.NormFloat64()*l.stddev + l.mean
		if d >= l.min {
			return d, true
		}
	}
	log.Panicf("not find a value for simualtor %+v in 100 rounds", l)
	return 0, false
}

type event struct {
	happen float64
	msg    message
}

type eventCalendar struct {
	now  float64
	heap *eventHeap
}

func newEventCalendar() *eventCalendar {
	h := &eventHeap{}
	heap.Init(h)
	return &eventCalendar{heap: h}
}

func (ec *eventCalendar) add(msg message, after float64) {
	heap.Push(ec.heap, event{happen: ec.now + after, msg: msg})
}

func (ec *eventCalendar) next() float64 {
	return (*ec.heap)[0].happen
}

func (ec *eventCalendar) advance() message {
	ev := heap.Pop(ec.heap).(event)
	ec.now = ev.happen
	// log.Printf("%+v arrives target at %v", ev.msg, ec.now)
	return ev.msg
}

func (ec *eventCalendar) len() int { return ec.heap.Len() }

type pair struct {
	from, to int
}

type cluster struct {
	nodes    map[int]*fsm
	halts    map[int]bool
	links    map[pair]link
	calendar *eventCalendar
}

func newCluster(n int) *cluster {
	peers := make([]int, n)
	for i := range peers {
		peers[i] = i + 1
	}
	cl := &cluster{
		nodes:    make(map[int]*fsm),
		halts:    make(map[int]bool),
		links:    make(map[pair]link),
		calendar: newEventCalendar(),
	}
	for _, id := range peers {
		cl.nodes[id] = &fsm{
			id:    id,
			peers: peers,
			nt:    cl,
		}
		for _, id2 := range peers {
			cl.links[pair{id, id2}] = link{
				min:    0,
				mean:   1,
				stddev: 0,
			}
		}
	}
	return cl
}

func (c *cluster) down(id int) { c.halts[id] = true }

func (c *cluster) up(id int) {
	c.halts[id] = false
	c.nodes[id] = c.nodes[id].reboot()
}

func (c *cluster) cont() {
	for c.calendar.len() > 0 {
		msg := c.calendar.advance()
		if !c.halts[msg.to] {
			c.nodes[msg.to].recv(msg)
		}
	}
}

func (c *cluster) contUntil(t float64) {
	for c.calendar.len() > 0 {
		if c.calendar.next() > t {
			return
		}
		msg := c.calendar.advance()
		if !c.halts[msg.to] {
			c.nodes[msg.to].recv(msg)
		}
	}
}

func (c *cluster) send(msg message) {
	if c.halts[msg.from] {
		return
	}
	if delay, ok := c.links[pair{msg.from, msg.to}].transfer(); ok {
		c.calendar.add(msg, delay)
	}
}

type eventHeap []event

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].happen < h[j].happen }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x interface{}) { *h = append(*h, x.(event)) }

func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
