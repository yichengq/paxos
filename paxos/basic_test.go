package paxos

import "testing"

func TestPropose(t *testing.T) {
	c := newCluster(3)
	c.nodes[1].propose(1, []byte("hello"))
	c.cont()

	if v, ok := c.nodes[1].chosenValue(); !ok || string(v) != "hello" {
		t.Errorf("failed to commit 'hello' proposal: %s, %v", v, ok)
	}
}

func TestProposeWhenMinorityAcceptorDown(t *testing.T) {
	c := newCluster(3)
	c.down(3)
	c.nodes[1].propose(1, []byte("hello"))
	c.cont()

	if v, ok := c.nodes[1].chosenValue(); !ok || string(v) != "hello" {
		t.Errorf("failed to commit 'hello' proposal: %s, %v", v, ok)
	}
}

func TestProposeFailWhenMajorityAcceptorDown(t *testing.T) {
	c := newCluster(3)
	c.down(2)
	c.down(3)
	c.nodes[1].propose(1, []byte("hello"))
	c.cont()

	if v, ok := c.nodes[1].chosenValue(); ok {
		t.Errorf("committed 'hello' proposal unexpectedly: %s, %v", v, ok)
	}
}

func TestProposeAgainAfterFirstProposerDie(t *testing.T) {
	c := newCluster(3)
	c.nodes[1].propose(1, []byte("hello"))
	c.contUntil(1)
	c.down(1)
	c.cont()
	if v, ok := c.nodes[1].chosenValue(); ok {
		t.Errorf("committed 'hello' proposal unexpectedly: %s, %v", v, ok)
	}

	c.nodes[2].propose(2, []byte("hello2"))
	c.cont()
	if v, ok := c.nodes[2].chosenValue(); !ok || string(v) != "hello2" {
		t.Errorf("failed to commit 'hello2' proposal: %s, %v", v, ok)
	}
}

func TestProposeDueling(t *testing.T) {
	c := newCluster(3)
	seq := 0
	for i := 0; i < 100; i++ {
		// old leader proposes
		seq++
		c.nodes[1].propose(seq, []byte("hello"))
		c.contUntil(float64(seq))
		if v, ok := c.nodes[1].chosenValue(); ok {
			t.Errorf("committed 'hello' proposal unexpectedly: %s, %v", v, ok)
		}

		// new leader proposes
		seq++
		c.nodes[2].propose(seq, []byte("hello2"))
		c.contUntil(float64(seq))
		if v, ok := c.nodes[2].chosenValue(); ok {
			t.Errorf("committed 'hello' proposal unexpectedly: %s, %v", v, ok)
		}
	}

	seq++
	c.nodes[1].propose(seq, []byte("hello"))
	c.cont()
	if v, ok := c.nodes[1].chosenValue(); !ok || string(v) != "hello" {
		t.Errorf("failed to commit 'hello' proposal: %s, %v", v, ok)
	}
}
