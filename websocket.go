package psyche

import (
	"context"
	"io"
	"log"
	"net/http"

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

func (h *WebsocketHandler) writeToConn(ctx context.Context, c *websocket.Conn, e Interface) {
	var buf []byte
	buf = EncodeInfo(buf[:0], e.Info())
	if err := c.Write(ctx, websocket.MessageBinary, buf); err != nil {
		log.Println(err)
		return
	}

	for {
		m, err := e.ReadMsg(ctx)
		if err != nil {
			log.Println(err)
			return
		}

		buf = EncodeMsg(buf[:0], m)
		if err = c.Write(ctx, websocket.MessageBinary, buf); err != nil {
			log.Println(err)
			return
		}
	}
}

func (h *WebsocketHandler) readFromConn(ctx context.Context, c *websocket.Conn, e Interface) {
	dec := NewDecoder(e)
	for {
		_, r, err := c.Reader(ctx)
		if err != nil {
			log.Println(err)
			return
		}

		_, err = io.Copy(dec, r)
		if err != nil {
			log.Println(err)
			return
		}
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

	ctx := r.Context()

	go h.readFromConn(ctx, c, e)
	h.writeToConn(ctx, c, e)
}
