package tf

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultClientHelloFragmentMin      = 100
	defaultClientHelloFragmentMax      = 200
	defaultClientHelloFragmentSleepMin = 10
	defaultClientHelloFragmentSleepMax = 20
)

// ClientHelloConfig controls Xray-compatible TLS ClientHello record
// fragmentation. Length values exclude the five-byte TLS record header.
type ClientHelloConfig struct {
	LengthMin   int
	LengthMax   int
	IntervalMin time.Duration
	IntervalMax time.Duration
}

func ParseClientHelloConfig(size, sleep string) (ClientHelloConfig, error) {
	lengthMin, lengthMax, err := parsePositiveRange(size, defaultClientHelloFragmentMin, defaultClientHelloFragmentMax)
	if err != nil {
		return ClientHelloConfig{}, fmt.Errorf("invalid TLS fragment size: %w", err)
	}
	intervalMin, intervalMax, err := parseNonNegativeRange(sleep, defaultClientHelloFragmentSleepMin, defaultClientHelloFragmentSleepMax)
	if err != nil {
		return ClientHelloConfig{}, fmt.Errorf("invalid TLS fragment sleep: %w", err)
	}
	return ClientHelloConfig{
		LengthMin:   lengthMin,
		LengthMax:   lengthMax,
		IntervalMin: time.Duration(intervalMin) * time.Millisecond,
		IntervalMax: time.Duration(intervalMax) * time.Millisecond,
	}, nil
}

func parsePositiveRange(value string, defaultMin, defaultMax int) (int, int, error) {
	min, max, err := parseRange(value, defaultMin, defaultMax)
	if err == nil && min < 1 {
		err = fmt.Errorf("range values must be positive")
	}
	if err == nil && max > 65535 {
		err = fmt.Errorf("range values must fit a TLS record")
	}
	return min, max, err
}

func parseNonNegativeRange(value string, defaultMin, defaultMax int) (int, int, error) {
	min, max, err := parseRange(value, defaultMin, defaultMax)
	if err == nil && min < 0 {
		err = fmt.Errorf("range values must not be negative")
	}
	return min, max, err
}

func parseRange(value string, defaultMin, defaultMax int) (int, int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultMin, defaultMax, nil
	}
	fromText, toText, ranged := strings.Cut(value, "-")
	if strings.Contains(toText, "-") {
		return 0, 0, fmt.Errorf("too many separators in %q", value)
	}
	from, err := strconv.Atoi(strings.TrimSpace(fromText))
	if err != nil {
		return 0, 0, err
	}
	to := from
	if ranged {
		to, err = strconv.Atoi(strings.TrimSpace(toText))
		if err != nil {
			return 0, 0, err
		}
	}
	if from > to {
		from, to = to, from
	}
	return from, to, nil
}

// ClientHelloConn transforms only the first complete TLS handshake record.
// Its payload is emitted as multiple valid TLS records, matching Xray's
// "packets: tlshello" behavior. All later writes are transparent.
type ClientHelloConn struct {
	net.Conn
	ctx    context.Context
	config ClientHelloConfig
	access sync.Mutex
	wrote  bool
}

func NewClientHelloConn(conn net.Conn, ctx context.Context, config ClientHelloConfig) *ClientHelloConn {
	return &ClientHelloConn{Conn: conn, ctx: ctx, config: config}
}

func (c *ClientHelloConn) Write(payload []byte) (int, error) {
	c.access.Lock()
	defer c.access.Unlock()
	if c.wrote {
		return c.Conn.Write(payload)
	}
	c.wrote = true
	if len(payload) <= recordLayerHeaderLen || payload[0] != contentType {
		return c.Conn.Write(payload)
	}
	recordLen := recordLayerHeaderLen + int(binary.BigEndian.Uint16(payload[3:5]))
	if len(payload) < recordLen {
		return c.Conn.Write(payload)
	}

	recordPayload := payload[recordLayerHeaderLen:recordLen]
	if len(recordPayload) == 0 {
		return c.Conn.Write(payload)
	}
	for offset := 0; offset < len(recordPayload); {
		fragmentLength := randomBetween(c.config.LengthMin, c.config.LengthMax)
		end := offset + fragmentLength
		if end > len(recordPayload) {
			end = len(recordPayload)
		}
		fragment := make([]byte, recordLayerHeaderLen+end-offset)
		copy(fragment[:3], payload[:3])
		binary.BigEndian.PutUint16(fragment[3:5], uint16(end-offset))
		copy(fragment[recordLayerHeaderLen:], recordPayload[offset:end])
		if err := writeFull(c.Conn, fragment); err != nil {
			return 0, err
		}
		offset = end
		if offset < len(recordPayload) {
			if err := c.wait(); err != nil {
				return 0, err
			}
		}
	}
	if len(payload) > recordLen {
		if err := writeFull(c.Conn, payload[recordLen:]); err != nil {
			return recordLen, err
		}
	}
	return len(payload), nil
}

func (c *ClientHelloConn) wait() error {
	delay := time.Duration(randomBetween(int(c.config.IntervalMin), int(c.config.IntervalMax)))
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-c.ctx.Done():
		return c.ctx.Err()
	}
}

func randomBetween(min, max int) int {
	if min == max {
		return min
	}
	return rand.Intn(max-min+1) + min
}

func writeFull(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := writer.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func (c *ClientHelloConn) ReaderReplaceable() bool { return true }
func (c *ClientHelloConn) WriterReplaceable() bool { return c.wrote }
func (c *ClientHelloConn) Upstream() any           { return c.Conn }
