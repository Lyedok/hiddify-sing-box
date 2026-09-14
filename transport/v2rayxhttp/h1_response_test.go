package xhttp

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

func TestH1PostPacketWaitsForAndDrainsResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	var dialOnce sync.Once
	client := &DefaultDialerClient{
		options:       &option.V2RayXHTTPBaseOptions{},
		httpVersion:   "1.1",
		uploadRawPool: &sync.Pool{},
		dialUploadConn: func(context.Context) (net.Conn, error) {
			var conn net.Conn
			dialOnce.Do(func() { conn = clientConn })
			if conn == nil {
				t.Fatal("unexpected second HTTP/1.1 connection")
			}
			return conn, nil
		},
	}

	requestRead := make(chan struct{})
	releaseResponse := make(chan struct{})
	serverDone := make(chan error, 1)
	go func() {
		request, err := http.ReadRequest(NewH1Conn(serverConn).RespBufReader)
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
		close(requestRead)
		<-releaseResponse
		_, err = io.WriteString(serverConn, "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nready")
		serverDone <- err
	}()

	result := make(chan error, 1)
	go func() {
		result <- client.PostPacket(context.Background(), "http://example.invalid/upload", bytes.NewReader([]byte("payload")), 7)
	}()

	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive the upload")
	}
	select {
	case err := <-result:
		t.Fatalf("PostPacket returned before its HTTP response was available: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseResponse)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("PostPacket did not finish after the response")
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestH1PostPacketCancellationClosesConnection(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()

	client := &DefaultDialerClient{
		options:       &option.V2RayXHTTPBaseOptions{},
		httpVersion:   "1.1",
		uploadRawPool: &sync.Pool{},
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

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.PostPacket(ctx, "http://example.invalid/upload", bytes.NewReader([]byte("payload")), 7)
	}()
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive the upload")
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("cancellation did not unblock HTTP/1.1 response read")
	}
	if pooled := client.uploadRawPool.Get(); pooled != nil {
		t.Fatal("cancelled HTTP/1.1 connection was returned to the pool")
	}
}
