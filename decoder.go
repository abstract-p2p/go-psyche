package psyche

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	opPub   = "PUB"
	opSub   = "SUB"
	opUnsub = "UNSUB"

	sepSpace      = []byte(" ")
	sepRetNewLine = []byte("\r\n")
	sepNewLine    = []byte("\n")

	opFormatPub   = "PUB <subject> <#bytes>\n<payload>\n"
	opFormatSub   = "SUB <subject>\n"
	opFormatUnsub = "UNSUB <subject>\n"
)

type DecoderError struct {
	Expected []string
	Actual   string
}

func (err DecoderError) Error() string {
	if len(err.Expected) == 1 {
		return fmt.Sprintf("expected %q, but got %q", err.Expected[0], err.Actual)
	}

	exp := strings.Join(err.Expected, ", ")
	return fmt.Sprintf("expected one of %s, but got %s", exp, err.Actual)
}

var ErrWriterClosed = errors.New("writer is closed")

func indexAny(bts []byte, seps [][]byte) (int, []byte) {
	for _, s := range seps {
		i := bytes.Index(bts, s)
		if i >= 0 {
			return i, s
		}
	}
	return -1, nil
}

type Decoder struct {
	edge Interface

	buf *bytes.Buffer

	opType     string
	subject    string
	payloadLen int
	payload    []byte

	protocolError  bool
	copyingPayload bool
	readingFromBuf bool
}

func NewDecoder(edge Interface) *Decoder {
	return &Decoder{
		edge: edge,

		buf:        new(bytes.Buffer),
		payloadLen: -1,
	}
}

func (d *Decoder) copyPayload() {
	chunk := d.buf.Next(d.payloadLen)
	d.payload = append(d.payload, chunk...)
	d.payloadLen -= len(chunk)

	if d.payloadLen == 0 {
		d.edge.Pub(d.subject, d.payload)
		d.copyingPayload = false
	}
}

func (d *Decoder) parseLine() error {
	idx, sep := indexAny(
		d.buf.Bytes(),
		[][]byte{sepRetNewLine, sepNewLine},
	)

	if idx < 0 {
		d.readingFromBuf = false
		return nil
	}

	line := d.buf.Next(idx)
	d.buf.Next(len(sep))

	parts := bytes.Split(line, sepSpace)
	opType := string(parts[0])

	switch opType {
	case opPub:
		if len(parts) != 3 {
			return DecoderError{
				Actual:   string(line),
				Expected: []string{opFormatPub},
			}
		}

		i, convErr := strconv.Atoi(string(parts[2]))
		if convErr != nil {
			return DecoderError{
				Actual:   string(line),
				Expected: []string{opFormatPub},
			}
		}

		d.opType = opPub
		d.subject = string(parts[1])
		d.payloadLen = i
		d.payload = d.payload[:0]
		d.copyingPayload = true

	case opSub:
		if len(parts) != 2 {
			return DecoderError{
				Actual:   string(line),
				Expected: []string{opFormatSub},
			}
		}

		d.edge.Sub(string(parts[1]))

	case opUnsub:
		if len(parts) != 2 {
			return DecoderError{
				Actual:   string(line),
				Expected: []string{opFormatUnsub},
			}
		}

		d.edge.Unsub(string(parts[1]))

	default:
		return DecoderError{
			Actual:   d.opType,
			Expected: []string{opPub, opSub, opUnsub},
		}
	}

	return nil
}

func (d *Decoder) Write(p []byte) (n int, err error) {
	if d.protocolError {
		return 0, ErrWriterClosed
	}

	n, err = d.buf.Write(p)
	if err != nil {
		return
	}

	d.readingFromBuf = true

	for d.readingFromBuf {
		if d.copyingPayload {
			d.copyPayload()
		} else {
			err = d.parseLine()
		}
	}

	if err != nil {
		d.protocolError = true
	}

	return
}
