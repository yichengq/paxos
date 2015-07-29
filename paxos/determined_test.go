package paxos

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"testing"
)

func TestProposeUsingSeed(t *testing.T) {
	for i := 312015; i < math.MaxInt64; i++ {
		log.Printf("start round using seed %d", i)
		testOneRound(t, int64(i))
	}
}

func testOneRound(t *testing.T, seed int64) {
	rand.Seed(seed)
	n := rand.Intn(9) + 3
	cl := newCluster(n)

	// start in halt state in 1%
	for id := 1; id <= n; id++ {
		if rand.Float64() < 0.01 {
			cl.down(id)
		}
	}

	for id := 1; id <= n; id++ {
		for id2 := 1; id2 <= n; id2++ {
			// mean is evenly distributed from [100, 1000)
			mean := rand.Float64()*900.0 + 100.0
			cl.links[pair{id, id2}] = link{
				// drop rate of each link is evenly distributed from [0, 0.99)
				drop: rand.Float64() * 0.99,
				// min is evenly distributed from [100, mean)
				min:    rand.Float64()*(mean-100) + 100.0,
				mean:   mean,
				stddev: mean / 4,
			}
		}
	}

	time := 0.0
	seq := 0

	seq++
	cl.nodes[1].propose(seq, []byte(fmt.Sprint("hello", seq)))
	for {
		time += 1.0
		cl.contUntil(time)
		for id := 1; id <= n; id++ {
			// each step may change one machine state in 0.01%
			if rand.Float64() < 0.0001 {
				cl.down(id)
			}
			if rand.Float64() < 0.0001 {
				cl.up(id)
			}
			// one machine will propose in 0.01%
			if rand.Float64() < 0.0001 {
				seq++
				cl.nodes[id].propose(seq, []byte(fmt.Sprint("hello", seq)))
			}
		}

		// end if all alive machines learn the same proposed value
		succ := true
		var pv string
		for id := 1; id <= n; id++ {
			if cl.halts[id] {
				continue
			}
			v, ok := cl.nodes[id].chosenValue()
			if !ok {
				succ = false
				break
			}
			if pv != "" && string(v) != pv {
				t.Fatalf("#%d: different chosen values from different machines", seed)
			}
		}
		if succ {
			return
		}
	}
}
