package psyche

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockEdge struct {
	mock.Mock
}

func NewMockEdge() *MockEdge {
	m := &MockEdge{}
	m.On("Pub", mock.Anything, mock.Anything)
	m.On("Sub", mock.Anything)
	m.On("Unsub", mock.Anything)
	return m
}

func (m *MockEdge) Pub(subject string, payload []byte) {
	m.Called(subject, payload)
}

func (m *MockEdge) Sub(subject string) {
	m.Called(subject)
}

func (m *MockEdge) Unsub(subject string) {
	m.Called(subject)
}

func (m *MockEdge) Info() map[string]interface{} {
	return nil
}

func (m *MockEdge) ReadMsg(ctx context.Context) (*Message, error) {
	return nil, nil
}

func (m *MockEdge) Close() {}

func TestDecoder(t *testing.T) {
	testCases := []struct {
		in     string
		expect func(t *testing.T, m *MockEdge)
	}{{
		in: "PUB foo 3\nbar\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Pub", "foo", []byte("bar"))
		},
	}, {
		in: "PUB foo 3\r\nbar\r\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Pub", "foo", []byte("bar"))
		},
	}, {
		in: "SUB foo\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Sub", "foo")
		},
	}, {
		in: "SUB foo\r\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Sub", "foo")
		},
	}, {
		in: "UNSUB foo\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Unsub", "foo")
		},
	}, {
		in: "UNSUB foo\r\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Unsub", "foo")
		},
	}, {
		in: "SUB foo\nUNSUB bar\n",
		expect: func(t *testing.T, m *MockEdge) {
			m.AssertCalled(t, "Sub", "foo")
			m.AssertCalled(t, "Unsub", "bar")
		},
	}}

	for i, tc := range testCases {
		m := NewMockEdge()
		dec := NewDecoder(m)

		_, err := io.Copy(dec, bytes.NewReader([]byte(tc.in)))
		tc.expect(t, m)

		assert.Nil(t, err, i)
	}

}

func TestDecoderInvalidOp(t *testing.T) {
	decoder := NewDecoder(nil)
	_, err := io.Copy(decoder, bytes.NewReader([]byte("NOSUCHOP foo bar\n")))

	assert.NotNil(t, DecoderError{
		Expected: []string{opPub, opSub, opUnsub},
		Actual:   "NOSUCHOP",
	}, err)
}

// func TestDecoderNoNewLines(t *testing.T) {
// 	decoder := NewDecoder(nil)

// 	_, err := io.Copy(decoder, bytes.NewReader([]byte("SUB fooUNSUB foo")))
// 	assert.Equal(t, DecoderError{
// 		Expected: []string{opFormatSub},
// 		Actual:   "SUB fooUNSUB foo",
// 	}, err)

// 	decoder = NewDecoder(nil)

// 	_, err = io.Copy(decoder, bytes.NewReader([]byte("PUB foo 3barSUB foo")))
// 	assert.Equal(t, &strconv.NumError{
// 		Func: "Atoi",
// 		Num:  "3barSUB",
// 		Err:  strconv.ErrSyntax,
// 	}, err)
// }
