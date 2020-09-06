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
	Pub(subject string, payload []byte)
	Sub(subject string)
	Unsub(subject string)
	Info() map[string]interface{}
	ReadMsg(ctx context.Context) (*Message, error)
	Close()
}

type Node struct {
	info           map[string]interface{}
	ctx            context.Context
	connectedEdges []Interface
	createdEdges   []*pipe
	mu             sync.Mutex
}

func NewNode(info map[string]interface{}) *Node {
	return &Node{
		info: info,
		ctx:  context.TODO(),
	}
}

// E.g. to serve a websocket connection
func (n *Node) NewEdge() Interface {
	pipe := newPipe(n.info)
	n.createdEdges = append(n.createdEdges, pipe)
	go n.acceptPubs(pipe)
	return pipe
}

// E.g. connect to LAN multicast
func (n *Node) Attach(edge Interface) {
	n.connectedEdges = append(n.connectedEdges, edge)
	go n.acceptMsgs(edge)
}

func (n *Node) broadcastMsg(m *Message, fromEdge Interface, fromPipe *pipe) {
	n.mu.Lock()
	for _, e := range n.connectedEdges {
		if e == fromEdge {
			continue
		}
		e.Pub(m.Subject, m.Payload)
	}
	for _, e := range n.createdEdges {
		if e == fromPipe {
			continue
		}
		e.msg(m.Subject, m.Payload)
	}
	n.mu.Unlock()
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
	defer n.removeEdge(edge)
	for {
		m, err := edge.ReadMsg(n.ctx)
		if err != nil {
			return
		}
		n.broadcastMsg(m, edge, nil)
	}
}

func (n *Node) removePipe(p *pipe) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, e := range n.createdEdges {
		if e == p {
			sl := n.createdEdges
			sl[i] = sl[len(sl)-1]
			sl = sl[:len(sl)-1]
			n.createdEdges = sl
			return
		}
	}
}

func (n *Node) removeEdge(edge Interface) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for i, e := range n.connectedEdges {
		if e == edge {
			sl := n.connectedEdges
			sl[i] = sl[len(sl)-1]
			sl = sl[:len(sl)-1]
			n.connectedEdges = sl
			return
		}
	}
}
