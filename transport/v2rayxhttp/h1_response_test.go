package xhttp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

func TestH1PostPacketDrainsPreviousResponseBeforeReuse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	var dialOnce sync.Once
	client := &DefaultDialerClient{
		options:       &option.V2RayXHTTPBaseOptions{},
		httpVersion:   "1.1",
		uploadRawPool: newH1ConnPool(),
		dialUploadConn: func(context.Context) (net.Conn, error) {
			var conn net.Conn
			dialOnce.Do(func() { conn = clientConn })
			if conn == nil {
				t.Fatal("unexpected second HTTP/1.1 connection")
			}
			return conn, nil
		},
	}

	firstRead := make(chan struct{})
	secondRead := make(chan struct{})
	releaseFirstResponse := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		reader := NewH1Conn(serverConn).RespBufReader
		request, err := http.ReadRequest(reader)
		if err != nil {
			serverDone <- err
			return
		}
		_, err = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if err != nil {
			serverDone <- err
			return
		}
		close(firstRead)
		<-releaseFirstResponse
		if _, err = io.WriteString(serverConn, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready"); err != nil {
			serverDone <- err
			return
		}
		request, err = http.ReadRequest(reader)
		if err != nil {
			serverDone <- err
			return
		}
		_, err = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		if err == nil {
			close(secondRead)
		}
		serverDone <- err
	}()

	if err := client.PostPacket(context.Background(), "http://example.invalid/upload/0", bytes.NewReader([]byte("first")), 5); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive the first upload")
	}

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- client.PostPacket(context.Background(), "http://example.invalid/upload/1", bytes.NewReader([]byte("second")), 6)
	}()
	select {
	case err := <-secondResult:
		t.Fatalf("second upload returned before the previous response: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirstResponse)
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second upload did not continue after draining the previous response")
	}
	select {
	case <-secondRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive the second upload")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
	client.uploadRawPool.Close()
}

func TestH1PostPacketCancellationClosesConnection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	client := &DefaultDialerClient{
		options:       &option.V2RayXHTTPBaseOptions{},
		httpVersion:   "1.1",
		uploadRawPool: newH1ConnPool(),
		dialUploadConn: func(context.Context) (net.Conn, error) {
			return clientConn, nil
		},
	}

	requestRead := make(chan struct{})
	go func() {
		request, err := http.ReadRequest(NewH1Conn(serverConn).RespBufReader)
		if err == nil {
			_, _ = io.Copy(io.Discard, request.Body)
			_ = request.Body.Close()
			close(requestRead)
		}
	}()
	if err := client.PostPacket(context.Background(), "http://example.invalid/upload/0", bytes.NewReader([]byte("first")), 5); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive the first upload")
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.PostPacket(ctx, "http://example.invalid/upload/1", bytes.NewReader([]byte("second")), 6)
	}()
	select {
	case err := <-result:
		t.Fatalf("second upload did not wait for the pending response: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cancellation did not unblock pending HTTP/1.1 response read")
	}
	if pooled := client.uploadRawPool.Get(); pooled != nil {
		t.Fatal("cancelled HTTP/1.1 connection was returned to the pool")
	}
}

type closeProbeConn struct {
	closed atomic.Bool
}

func (c *closeProbeConn) Read([]byte) (int, error)          { return 0, net.ErrClosed }
func (c *closeProbeConn) Write(payload []byte) (int, error) { return len(payload), nil }
func (c *closeProbeConn) Close() error {
	c.closed.Store(true)
	return nil
}
func (c *closeProbeConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *closeProbeConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *closeProbeConn) SetDeadline(time.Time) error      { return nil }
func (c *closeProbeConn) SetReadDeadline(time.Time) error  { return nil }
func (c *closeProbeConn) SetWriteDeadline(time.Time) error { return nil }

func TestH1PoolOwnsAndClosesIdleConnections(t *testing.T) {
	pool := &h1ConnPool{maxIdle: 2}
	first := &closeProbeConn{}
	second := &closeProbeConn{}
	overflow := &closeProbeConn{}
	pool.Put(NewH1Conn(first))
	pool.Put(NewH1Conn(second))
	pool.Put(NewH1Conn(overflow))
	if !overflow.closed.Load() {
		t.Fatal("connection beyond the idle limit was not closed")
	}
	if first.closed.Load() || second.closed.Load() {
		t.Fatal("connection within the idle limit was closed early")
	}

	pool.Close()
	pool.Close()
	if !first.closed.Load() || !second.closed.Load() {
		t.Fatal("pool shutdown did not close all idle connections")
	}
	afterClose := &closeProbeConn{}
	pool.Put(NewH1Conn(afterClose))
	if !afterClose.closed.Load() {
		t.Fatal("connection returned after pool shutdown was not closed")
	}
}
