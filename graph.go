package psyche

import (
	"context"
	"errors"
	"sync"
)

var ErrInterfaceClosed = errors.New("psyche interface is closed")

type Message struct {
	Subject string
	Payload []byte
}

type Interface interface {
	Info() map[string]interface{}
	Pub(subject string, payload []byte)
	Sub(subject string)
	Unsub(subject string)
	ReadMsg(ctx context.Context, msg *Message) bool
	Err() error
	Close()
}

type Node struct {
	info map[string]interface{}
	ctx  context.Context

	edges   []*pipe
	edgesMu sync.Mutex

	allowLocalDotSubjects bool
	gateway               Interface

	gatewaySubjects   map[string]int
	gatewaySubjectsMu sync.Mutex
}

type NodeOption func(n *Node)

// AllowLocalDotSubjects will allow subjects beginning with "." to be
// published, but only on non-gateway (i.e. local) edges.
func AllowLocalDotSubjects() func(n *Node) {
	return func(n *Node) {
		n.allowLocalDotSubjects = true
	}
}

// WithGateway sets a gateway edge, to which all PUBs, SUBs and UNSUBs
// received from locally created edges.
//
// The node will subscribe to a subject on gateway for as along as one
// or more locally created edges are subscribed to that subject.
func WithGateway(gw Interface) func(n *Node) {
	return func(n *Node) {
		n.gateway = gw
	}
}

func NewNode(info map[string]interface{}, opts ...NodeOption) *Node {
	n := &Node{
		info: info,
		ctx:  context.TODO(),
	}

	for _, opt := range opts {
		opt(n)
	}

	if n.gateway != nil {
		n.gatewaySubjects = map[string]int{}
		go n.acceptMsgs(n.gateway)
	}

	return n
}

// E.g. to serve a websocket connection
func (n *Node) NewEdge() Interface {
	pipe := newPipe(n.info, n)

	n.edgesMu.Lock()
	n.edges = append(n.edges, pipe)
	n.edgesMu.Unlock()

	go n.acceptPubs(pipe)
	return pipe
}

func (n *Node) broadcastMsg(m *Message, fromEdge Interface, fromPipe *pipe) {
	isDotSubject := m.Subject[0] == '.'

	// allow dot subjects only if explicitly enabled
	if isDotSubject && !n.allowLocalDotSubjects {
		return
	}

	n.edgesMu.Lock()
	defer n.edgesMu.Unlock()

	// send msg to the other edges
	for _, e := range n.edges {
		if e == fromPipe {
			continue
		}
		e.msg(m.Subject, m.Payload)
	}

	// never send dot subject messages to gateway
	if isDotSubject {
		return
	}

	// send msg to the gateway
	if n.gateway != fromEdge {
		n.gateway.Pub(m.Subject, m.Payload)
	}
}

func (n *Node) acceptPubs(p *pipe) {
	defer n.removePipe(p)
	for {
		m, err := p.readPub(n.ctx)
		if err != nil {
			return
		}
		n.broadcastMsg(m, nil, p)
	}
}

func (n *Node) acceptMsgs(edge Interface) {
	var msg Message
	for edge.ReadMsg(n.ctx, &msg) {
		m := &Message{
			Subject: msg.Subject,
			Payload: append(msg.Payload[:0:0], msg.Payload...),
		}

		n.broadcastMsg(m, edge, nil)
	}
}

func (n *Node) removePipe(p *pipe) {
	n.edgesMu.Lock()
	defer n.edgesMu.Unlock()
	for i, e := range n.edges {
		if e == p {
			sl := n.edges
			sl[i] = sl[len(sl)-1]
			sl = sl[:len(sl)-1]
			n.edges = sl
			return
		}
	}
}

func (n *Node) gatewaySub(subject string) {
	if n.gateway == nil {
		return
	}

	n.gatewaySubjectsMu.Lock()
	n.gatewaySubjects[subject]++
	count := n.gatewaySubjects[subject]
	n.gatewaySubjectsMu.Unlock()

	if count == 1 {
		n.gateway.Sub(subject)
	}
}

func (n *Node) gatewayUnsub(subject string) {
	if n.gateway == nil {
		return
	}

	n.gatewaySubjectsMu.Lock()
	n.gatewaySubjects[subject]--
	count := n.gatewaySubjects[subject]
	n.gatewaySubjectsMu.Unlock()

	if count == 0 {
		n.gateway.Unsub(subject)
	}
}
