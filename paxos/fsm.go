package paxos

type network interface {
	send(msg message)
}

type proposal struct {
	seq  int
	data []byte
}

func (p proposal) lessThan(a proposal) bool { return p.seq < a.seq }

func (p proposal) isEmpty() bool { return p.seq == 0 }

type fsm struct {
	id    int
	peers []int
	nt    network

	propSeq  int
	promises int
	reported proposal
	accepts  int
	chosen   proposal

	// these two fields need to be persisted to stable storage
	promiseSeq int
	accepted   proposal
}

func (sm *fsm) recv(msg message) {
	switch msg.typ {
	case msgPrepare:
		sm.handlePrepare(msg)
	case msgPromise:
		sm.handlePromise(msg)
	case msgAcceptRequest:
		sm.handleAcceptRequest(msg)
	case msgAccepted:
		sm.handleAccepted(msg)
	}
}

func (sm *fsm) propose(seq int, data []byte) {
	if sm.propSeq >= seq {
		panic("expect higher seq")
	}
	sm.propSeq = seq
	sm.promises = 0
	sm.reported = proposal{data: data}
	sm.bcast(message{
		typ: msgPrepare,
		seq: sm.propSeq,
	})
}

func (sm *fsm) chosenValue() ([]byte, bool) {
	if sm.chosen.isEmpty() {
		return nil, false
	} else {
		return sm.chosen.data, true
	}
}

func (sm *fsm) handlePrepare(msg message) {
	if msg.typ != msgPrepare {
		panic("invalid message type")
	}
	if sm.promiseSeq >= msg.seq {
		return
	}
	sm.promiseSeq = msg.seq
	sm.send(msg.from, message{
		typ:  msgPromise,
		seq:  msg.seq,
		prop: sm.accepted,
	})
}

func (sm *fsm) handlePromise(msg message) {
	if msg.typ != msgPromise {
		panic("invalid message type")
	}
	if sm.propSeq != msg.seq {
		return
	}
	sm.promises++
	if sm.reported.lessThan(msg.prop) {
		sm.reported = msg.prop
	}

	if sm.promises != sm.quorum() {
		return
	}
	sm.reported.seq = sm.propSeq
	sm.bcast(message{
		typ:  msgAcceptRequest,
		seq:  sm.propSeq,
		prop: sm.reported,
	})
}

func (sm *fsm) handleAcceptRequest(msg message) {
	if msg.typ != msgAcceptRequest {
		panic("invalid message type")
	}
	if sm.promiseSeq > msg.seq {
		return
	}
	sm.accepted = msg.prop
	sm.send(msg.from, message{
		typ: msgAccepted,
		seq: msg.seq,
	})
}

func (sm *fsm) handleAccepted(msg message) {
	if msg.typ != msgAccepted {
		panic("invalid message type")
	}
	if sm.propSeq != msg.seq {
		return
	}
	sm.accepts++
	if sm.accepts != sm.quorum() {
		return
	}
	sm.chosen = sm.reported
}

func (sm *fsm) bcast(msg message) {
	for _, to := range sm.peers {
		sm.send(to, msg)
	}
}

func (sm *fsm) send(to int, msg message) {
	msg.from = sm.id
	msg.to = to
	sm.nt.send(msg)
}

func (sm *fsm) quorum() int { return len(sm.peers)/2 + 1 }
