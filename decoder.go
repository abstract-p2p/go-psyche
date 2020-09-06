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

	payloadPlaceHolder = "<payload>"
	payloadFormat      = payloadPlaceHolder + "\n"
	opFormatPub        = "PUB <subject> <#bytes>\n" + payloadFormat
	opFormatSub        = "SUB <subject>\n"
	opFormatUnsub      = "UNSUB <subject>\n"
)

type DecoderError struct {
	Expected []string
	Actual   string
}

func (err DecoderError) Error() string {
	if len(err.Expected) == 1 {
		return fmt.Sprintf("bad input: %q, use format: %q", err.Actual, err.Expected[0])
	}

	exp := strings.Join(err.Expected, ", ")
	return fmt.Sprintf("bad input: %s, use one of %s", err.Actual, exp)
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
	moreToParse    bool
}

func NewDecoder(edge Interface) *Decoder {
	return &Decoder{
		edge: edge,

		buf:        new(bytes.Buffer),
		payloadLen: -1,
	}
}

func (d *Decoder) closePayload() error {
	// find separator
	i, sep := indexAny(
		d.buf.Bytes(),
		[][]byte{sepRetNewLine, sepNewLine},
	)

	// validate separator
	if i > 0 || (i < 0 && len(d.buf.Bytes()) >= 2) {
		return DecoderError{
			Expected: []string{payloadFormat},
			Actual:   payloadPlaceHolder + string(d.buf.Bytes()[:2]),
		}
	} else if i < 0 {
		d.moreToParse = false
		return nil
	}

	// read separator
	d.buf.Next(len(sep))

	// publish payload
	d.edge.Pub(d.subject, d.payload)
	d.copyingPayload = false
	return nil
}

func (d *Decoder) copyPayload() error {
	if d.payloadLen == 0 {
		return d.closePayload()
	}

	// read from buf
	chunk := d.buf.Next(d.payloadLen)
	if len(chunk) == 0 {
		d.moreToParse = false
		return nil
	}

	// append to payload
	d.payload = append(d.payload, chunk...)
	d.payloadLen -= len(chunk)

	if d.payloadLen == 0 {
		return d.closePayload()
	}

	return nil
}

func (d *Decoder) parseLine() error {
	idx, sep := indexAny(
		d.buf.Bytes(),
		[][]byte{sepRetNewLine, sepNewLine},
	)

	if idx < 0 {
		d.moreToParse = false
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
				Expected: []string{opFormatPub},
				Actual:   string(line),
			}
		}

		i, convErr := strconv.Atoi(string(parts[2]))
		if convErr != nil {
			return DecoderError{
				Expected: []string{opFormatPub},
				Actual:   string(line),
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
				Expected: []string{opFormatSub},
				Actual:   string(line),
			}
		}

		d.edge.Sub(string(parts[1]))

	case opUnsub:
		if len(parts) != 2 {
			return DecoderError{
				Expected: []string{opFormatUnsub},
				Actual:   string(line),
			}
		}

		d.edge.Unsub(string(parts[1]))

	default:
		return DecoderError{
			Expected: []string{opPub, opSub, opUnsub},
			Actual:   opType,
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

	d.moreToParse = true

	for d.moreToParse {
		if d.copyingPayload {
			if err = d.copyPayload(); err != nil {
				d.protocolError = true
				return
			}
		} else {
			if err = d.parseLine(); err != nil {
				d.protocolError = true
				return
			}
		}
	}

	return
}
