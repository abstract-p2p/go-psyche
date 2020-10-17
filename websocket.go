package psyche

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"nhooyr.io/websocket"
)

type WebsocketHandler struct {
	node *Node
}

func NewWebsocketHandler(node *Node) *WebsocketHandler {
	return &WebsocketHandler{
		node: node,
	}
}

type handledConn struct {
	ctx  context.Context
	conn *websocket.Conn
	edge Interface

	readActivityCh chan struct{}
}

func (hc *handledConn) pingAndTimeout(interval time.Duration, maxOutstanding int) {
	var (
		pingTimer   = time.NewTimer(interval)
		pingMsg     = EncodePing(nil)
		outstanding int
	)
	defer pingTimer.Stop()

	for {
		select {
		case <-pingTimer.C:
			if outstanding >= maxOutstanding {
				hc.conn.Close(
					websocket.StatusGoingAway,
					fmt.Sprintf(
						"no response for %s",
						(interval*time.Duration(maxOutstanding+1)).String(),
					))
				return
			}

			if err := hc.conn.Write(hc.ctx, websocket.MessageBinary, pingMsg); err != nil {
				log.Println(err)
				return
			}
			outstanding++

			pingTimer.Reset(interval)

		case <-hc.readActivityCh:
			outstanding = 0

			if !pingTimer.Stop() {
				<-pingTimer.C
			}
			pingTimer.Reset(interval)

		case <-hc.ctx.Done():
			return
		}
	}
}

func (hc *handledConn) ping() {
	hc.conn.Write(hc.ctx, websocket.MessageBinary, EncodePong(nil))
}

func (hc *handledConn) readAndDecode() {
	dec := NewDecoder(hc.edge, PingerFunc(hc.ping))
	for {
		_, r, err := hc.conn.Reader(hc.ctx)
		if err != nil {
			log.Println(err)
			return
		}

		select {
		case hc.readActivityCh <- struct{}{}:
		default:
		}

		_, err = io.Copy(dec, r)

		if err != nil {
			hc.conn.Write(hc.ctx, websocket.MessageBinary, EncodeErr(nil, err))
			log.Println(err)
			return
		}
	}
}

func (hc *handledConn) encodeAndWrite() {
	var buf []byte
	buf = EncodeInfo(buf[:0], hc.edge.Info())
	if err := hc.conn.Write(hc.ctx, websocket.MessageBinary, buf); err != nil {
		log.Println(err)
		return
	}

	var m Message
	for hc.edge.ReadMsg(hc.ctx, &m) {
		buf = EncodeMsg(buf[:0], &m)
		if err := hc.conn.Write(hc.ctx, websocket.MessageBinary, buf); err != nil {
			log.Println(err)
			return
		}
	}

	if hc.edge.Err() != nil {
		log.Println(hc.edge.Err())
	}
}

func (h *WebsocketHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// TODO: don't
		InsecureSkipVerify: true,
	})
	if err != nil {
		log.Println(err)
		return
	}
	defer c.Close(websocket.StatusInternalError, "the sky is falling")

	e := h.node.NewEdge()
	defer e.Close()

	hc := &handledConn{
		ctx:            r.Context(),
		conn:           c,
		edge:           e,
		readActivityCh: make(chan struct{}, 1),
	}

	go hc.pingAndTimeout(30*time.Second, 2)
	go hc.encodeAndWrite()
	hc.readAndDecode()
}
