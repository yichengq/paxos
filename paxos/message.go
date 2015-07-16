package paxos

const (
	msgPrepare msgType = iota
	msgPromise
	msgAcceptRequest
	msgAccepted
)

type msgType int

type message struct {
	from, to int
	typ      msgType
	seq      int
	prop     proposal
}
