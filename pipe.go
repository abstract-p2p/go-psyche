package psyche

import (
	"context"
	"sync"
)

type pipe struct {
	info         map[string]interface{}
	pubCh, msgCh chan *Message
	subjects     map[string]struct{}
	mu           sync.Mutex
	closeCh      chan struct{}
	err          error
}

func newPipe(info map[string]interface{}) *pipe {
	return &pipe{
		info:     info,
		pubCh:    make(chan *Message, 8),
		msgCh:    make(chan *Message, 8),
		subjects: map[string]struct{}{},
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
}

func (p *pipe) Unsub(subject string) {
	p.mu.Lock()
	delete(p.subjects, subject)
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
	close(p.closeCh)
}

func (p *pipe) msg(subject string, payload []byte) {
	p.mu.Lock()
	_, ok := p.subjects[subject]
	p.mu.Unlock()
	if !ok {
		return
	}

	p.msgCh <- &Message{
		Subject: subject,
		Payload: payload,
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
