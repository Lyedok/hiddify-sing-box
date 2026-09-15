package xhttp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
)

type contextBlockingRoundTripper struct {
	started chan struct{}
	once    sync.Once
}

func (t *contextBlockingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.once.Do(func() { close(t.started) })
	<-request.Context().Done()
	return nil, request.Context().Err()
}

type connectedContextBlockingRoundTripper struct {
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (t *connectedContextBlockingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.GotConn != nil {
		trace.GotConn(httptrace.GotConnInfo{Conn: client})
	}
	t.once.Do(func() { close(t.started) })
	<-request.Context().Done()
	close(t.stopped)
	return nil, request.Context().Err()
}

func TestPostPacketHonorsCancellation(t *testing.T) {
	transport := &contextBlockingRoundTripper{started: make(chan struct{})}
	client := &DefaultDialerClient{
		options:     &option.V2RayXHTTPBaseOptions{},
		client:      &http.Client{Transport: transport},
		httpVersion: "2",
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- client.PostPacket(ctx, "https://example.invalid/xhttp", bytes.NewReader([]byte("x")), 1)
	}()

	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("packet request did not start")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("packet request ignored context cancellation")
	}
}

func TestOpenStreamHonorsCancellation(t *testing.T) {
	transport := &connectedContextBlockingRoundTripper{started: make(chan struct{}), stopped: make(chan struct{})}
	client := &DefaultDialerClient{
		options:     &option.V2RayXHTTPBaseOptions{},
		client:      &http.Client{Transport: transport},
		httpVersion: "2",
	}
	ctx, cancel := context.WithCancel(context.Background())
	reader, _, _, err := client.OpenStream(ctx, "https://example.invalid/xhttp", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("download request did not start")
	}
	cancel()
	select {
	case <-transport.stopped:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("download request ignored logical connection cancellation")
	}
}

type packetCancelProbeClient struct {
	started chan struct{}
	stopped chan struct{}
	once    sync.Once
}

func (c *packetCancelProbeClient) IsClosed() bool { return false }

func (c *packetCancelProbeClient) OpenStream(context.Context, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error) {
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}
	return io.NopCloser(bytes.NewReader(nil)), addr, addr, nil
}

func (c *packetCancelProbeClient) PostPacket(ctx context.Context, _ string, _ io.Reader, _ int64) error {
	c.once.Do(func() { close(c.started) })
	<-ctx.Done()
	close(c.stopped)
	return ctx.Err()
}

func TestPacketUpCloseCancelsInflightPost(t *testing.T) {
	probe := &packetCancelProbeClient{started: make(chan struct{}), stopped: make(chan struct{})}
	baseURL := url.URL{Scheme: "https", Host: "example.invalid", Path: "/xhttp"}
	options := &option.V2RayXHTTPOptions{Mode: "packet-up"}
	client := &Client{
		ctx:            context.Background(),
		options:        options,
		getRequestURL:  func(string) url.URL { return baseURL },
		getRequestURL2: func(string) url.URL { return baseURL },
		getHTTPClient:  func() (DialerClient, *XmuxClient) { return probe, nil },
		getHTTPClient2: func() (DialerClient, *XmuxClient) { return probe, nil },
	}

	connection, err := client.DialContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("packet upload did not start")
	}
	if err = connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.stopped:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("closing packet-up connection did not cancel in-flight POST")
	}
}

func TestEstablishedPacketUpSurvivesDialContextCancellation(t *testing.T) {
	probe := &packetCancelProbeClient{started: make(chan struct{}), stopped: make(chan struct{})}
	baseURL := url.URL{Scheme: "https", Host: "example.invalid", Path: "/xhttp"}
	options := &option.V2RayXHTTPOptions{Mode: "packet-up"}
	client := &Client{
		ctx:            context.Background(),
		options:        options,
		getRequestURL:  func(string) url.URL { return baseURL },
		getRequestURL2: func(string) url.URL { return baseURL },
		getHTTPClient:  func() (DialerClient, *XmuxClient) { return probe, nil },
		getHTTPClient2: func() (DialerClient, *XmuxClient) { return probe, nil },
	}

	dialCtx, cancelDial := context.WithCancel(context.Background())
	connection, err := client.DialContext(dialCtx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connection.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("packet upload did not start")
	}
	cancelDial()
	select {
	case <-probe.stopped:
		t.Fatal("established connection inherited cancellation from dial context")
	case <-time.After(100 * time.Millisecond):
	}
	if err = connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.stopped:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("closing established connection did not cancel packet upload")
	}
}

type closeIdleProbe struct {
	closed     bool
	closeCalls int
}

func (c *closeIdleProbe) IsClosed() bool { return c.closed }
func (c *closeIdleProbe) CloseIdleConnections() {
	c.closeCalls++
}

func TestXmuxPruneClosesIdleTransport(t *testing.T) {
	retired := &closeIdleProbe{closed: true}
	replacement := &closeIdleProbe{}
	manager := NewXmuxManager(option.V2RayXHTTPXmuxOptions{}, func() XmuxConn { return replacement })
	manager.xmuxClients = append(manager.xmuxClients, &XmuxClient{XmuxConn: retired})

	selected := manager.GetXmuxClient(context.Background())
	if selected.XmuxConn != replacement {
		t.Fatal("closed XMUX transport was selected again")
	}
	if retired.closeCalls != 1 {
		t.Fatalf("expected retired transport to close idle connections once, got %d", retired.closeCalls)
	}
}
