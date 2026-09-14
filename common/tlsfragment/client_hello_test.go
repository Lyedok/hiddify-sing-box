package tf

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"
)

type recordingConn struct {
	bytes.Buffer
}

func (c *recordingConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *recordingConn) Close() error                     { return nil }
func (c *recordingConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *recordingConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *recordingConn) SetDeadline(time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(time.Time) error { return nil }

func TestClientHelloConnFragmentsFirstTLSRecord(t *testing.T) {
	underlying := &recordingConn{}
	conn := NewClientHelloConn(underlying, context.Background(), ClientHelloConfig{
		LengthMin: 3,
		LengthMax: 3,
	})
	body := []byte("abcdefghij")
	record := make([]byte, 5+len(body))
	record[0], record[1], record[2] = 22, 3, 1
	binary.BigEndian.PutUint16(record[3:5], uint16(len(body)))
	copy(record[5:], body)
	input := append(record, []byte("tail")...)

	n, err := conn.Write(input)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(input) {
		t.Fatalf("wrote %d logical bytes, expected %d", n, len(input))
	}
	if _, err = conn.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}

	output := underlying.Bytes()
	var rebuilt []byte
	for _, expectedLength := range []int{3, 3, 3, 1} {
		if len(output) < 5 {
			t.Fatal("truncated fragmented record")
		}
		if output[0] != 22 || output[1] != 3 || output[2] != 1 {
			t.Fatalf("unexpected TLS record header: %x", output[:3])
		}
		length := int(binary.BigEndian.Uint16(output[3:5]))
		if length != expectedLength {
			t.Fatalf("fragment length %d, expected %d", length, expectedLength)
		}
		if len(output) < 5+length {
			t.Fatal("truncated fragmented payload")
		}
		rebuilt = append(rebuilt, output[5:5+length]...)
		output = output[5+length:]
	}
	if !bytes.Equal(rebuilt, body) {
		t.Fatalf("rebuilt ClientHello differs: %q", rebuilt)
	}
	if string(output) != "tailnext" {
		t.Fatalf("bytes after first TLS record were not transparent: %q", output)
	}
}

func TestClientHelloConnPassesIncompleteFirstRecord(t *testing.T) {
	underlying := &recordingConn{}
	conn := NewClientHelloConn(underlying, context.Background(), ClientHelloConfig{LengthMin: 2, LengthMax: 2})
	input := []byte{22, 3, 1, 0, 10, 1, 2}
	if _, err := conn.Write(input); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("next")); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(underlying.Bytes(), append(input, []byte("next")...)) {
		t.Fatal("incomplete first record was changed")
	}
}

func TestParseClientHelloConfig(t *testing.T) {
	config, err := ParseClientHelloConfig("200-100", "10-20")
	if err != nil {
		t.Fatal(err)
	}
	if config.LengthMin != 100 || config.LengthMax != 200 {
		t.Fatalf("unexpected length range: %+v", config)
	}
	if config.IntervalMin != 10*time.Millisecond || config.IntervalMax != 20*time.Millisecond {
		t.Fatalf("unexpected interval range: %+v", config)
	}
	for _, invalid := range [][2]string{{"0-10", "1"}, {"a-b", "1"}, {"10", "-1"}} {
		if _, err = ParseClientHelloConfig(invalid[0], invalid[1]); err == nil {
			t.Fatalf("accepted invalid ranges %q/%q", invalid[0], invalid[1])
		}
	}
}
