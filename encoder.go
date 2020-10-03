package psyche

import (
	"encoding/json"
	"fmt"
)

func EncodeInfo(out []byte, info map[string]interface{}) []byte {
	m, err := json.Marshal(info)
	if err != nil {
		return append(out, []byte(`INFO {"error": "error marshalling info"}\n`)...)
	}
	out = append(out, []byte("INFO ")...)
	out = append(out, m...)
	return append(out, '\n')
}

func EncodeMsg(out []byte, m *Message) []byte {
	out = append(out, []byte(fmt.Sprintf("MSG %s %d\n", m.Subject, len(m.Payload)))...)
	out = append(out, m.Payload...)
	return append(out, '\n')
}

func EncodeErr(out []byte, err error) []byte {
	return append(out, []byte(fmt.Sprintf("-ERR %s\n", err.Error()))...)
}

func EncodePing(out []byte) []byte {
	return append(out, []byte("PING")...)
}

func EncodePong(out []byte) []byte {
	return append(out, []byte("PONG")...)
}
