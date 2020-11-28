package psyche

import (
	"context"
	"sync"
)

type pipe struct {
	info         map[string]interface{}
	pubCh, msgCh chan *Message

	subjects map[string]struct{}
	mu       sync.Mutex

	parent *Node

	closeOnce sync.Once
	closeCh   chan struct{}
	err       error
}

func newPipe(info map[string]interface{}, parent *Node) *pipe {
	return &pipe{
		info:     info,
		pubCh:    make(chan *Message, 8),
		msgCh:    make(chan *Message, 8),
		subjects: map[string]struct{}{},
		parent:   parent,
		closeCh:  make(chan struct{}),
	}
}

func (p *pipe) Pub(subject string, payload []byte) {
	p.pubCh <- &Message{
		Subject: subject,
		Payload: payload,
	}
}

func (p *pipe) Sub(subject string) {
	p.mu.Lock()
	p.subjects[subject] = struct{}{}
	p.mu.Unlock()
	p.parent.gatewaySub(subject)
}

func (p *pipe) Unsub(subject string) {
	p.mu.Lock()
	delete(p.subjects, subject)
	p.mu.Unlock()
	p.parent.gatewayUnsub(subject)
}

func (p *pipe) unsubAll() {
	p.mu.Lock()
	for s := range p.subjects {
		delete(p.subjects, s)
		p.parent.gatewayUnsub(s)
	}
	p.mu.Unlock()
}

func (p *pipe) Info() map[string]interface{} {
	return p.info
}

func (p *pipe) ReadMsg(ctx context.Context, msg *Message) bool {
	if p.err != nil {
		return false
	}

	for {
		select {
		case m := <-p.msgCh:
			msg.Subject = m.Subject
			msg.Payload = append(msg.Payload[:0], m.Payload...)
			return true
		case <-p.closeCh:
			p.err = ErrInterfaceClosed
			return false
		case <-ctx.Done():
			p.err = ctx.Err()
			return false
		}
	}
}

func (p *pipe) Err() error {
	return p.err
}

func (p *pipe) Close() {
	p.unsubAll()
	p.closeOnce.Do(func() {
		close(p.closeCh)
	})
}

func (p *pipe) msg(subject string, payload []byte) error {
	p.mu.Lock()
	_, ok := p.subjects[subject]
	p.mu.Unlock()

	if !ok {
		// this edge is not subscribed to this subject
		return nil
	}

	select {
	case p.msgCh <- &Message{
		Subject: subject,
		Payload: payload,
	}:
		return nil
	case <-p.closeCh:
		return ErrInterfaceClosed
	}
}

func (p *pipe) readPub(ctx context.Context) (*Message, error) {
	select {
	case m := <-p.pubCh:
		return m, nil
	case <-p.closeCh:
		return nil, ErrInterfaceClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
