package xhttp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"

	common "github.com/sagernet/sing-box/common/xray"
	"github.com/sagernet/sing-box/common/xray/signal/done"
	"github.com/sagernet/sing-box/option"
)

// interface to abstract between use of browser dialer, vs net/http
type DialerClient interface {
	IsClosed() bool

	// ctx, url, body, uploadOnly
	OpenStream(context.Context, string, io.Reader, bool) (io.ReadCloser, net.Addr, net.Addr, error)

	// ctx, url, body, contentLength
	PostPacket(context.Context, string, io.Reader, int64) error
}

// implements xhttp.DialerClient in terms of direct network connections
type DefaultDialerClient struct {
	options     *option.V2RayXHTTPBaseOptions
	client      *http.Client
	closed      atomic.Bool
	httpVersion string
	// pool of net.Conn, created using dialUploadConn
	uploadRawPool  *h1ConnPool
	dialUploadConn func(ctxInner context.Context) (net.Conn, error)
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *DefaultDialerClient) CloseIdleConnections() {
	c.client.CloseIdleConnections()
	if c.uploadRawPool != nil {
		c.uploadRawPool.Close()
	}
}

const maxIdleH1UploadConnections = 16

type h1ConnPool struct {
	access      sync.Mutex
	connections []*H1Conn
	closed      bool
	maxIdle     int
}

func newH1ConnPool() *h1ConnPool {
	return &h1ConnPool{maxIdle: maxIdleH1UploadConnections}
}

func (p *h1ConnPool) Get() *H1Conn {
	p.access.Lock()
	defer p.access.Unlock()
	if len(p.connections) == 0 {
		return nil
	}
	last := len(p.connections) - 1
	conn := p.connections[last]
	p.connections = p.connections[:last]
	return conn
}

func (p *h1ConnPool) Put(conn *H1Conn) {
	p.access.Lock()
	if !p.closed && len(p.connections) < p.maxIdle {
		p.connections = append(p.connections, conn)
		p.access.Unlock()
		return
	}
	p.access.Unlock()
	_ = conn.Close()
}

func (p *h1ConnPool) Close() {
	p.access.Lock()
	if p.closed {
		p.access.Unlock()
		return
	}
	p.closed = true
	connections := p.connections
	p.connections = nil
	p.access.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
}

func (c *DefaultDialerClient) OpenStream(ctx context.Context, url string, body io.Reader, uploadOnly bool) (wrc io.ReadCloser, remoteAddr, localAddr net.Addr, err error) {
	// this is done when the TCP/UDP connection to the server was established,
	// and we can unblock the Dial function and print correct net addresses in
	// logs
	gotConn := done.New()
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			remoteAddr = connInfo.Conn.RemoteAddr()
			localAddr = connInfo.Conn.LocalAddr()
			gotConn.Close()
		},
	})
	method := "GET" // stream-down
	if body != nil {
		method = "POST" // stream-up/one
	}
	req, _ := http.NewRequestWithContext(ctx, method, url, body)
	req.Header = c.options.GetRequestHeader(url)
	if method == "POST" && !c.options.NoGRPCHeader {
		req.Header.Set("Content-Type", "application/grpc")
	}
	wrc = &WaitReadCloser{ctx: ctx, Wait: make(chan struct{})}
	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			if !uploadOnly { // stream-down is enough
				c.closed.Store(true)
			}
			gotConn.Close()
			wrc.Close()
			return
		}
		if resp.StatusCode != 200 || uploadOnly { // stream-up
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close() // if it is called immediately, the upload will be interrupted also
			wrc.Close()
			return
		}
		wrc.(*WaitReadCloser).Set(resp.Body)
	}()
	select {
	case <-gotConn.Wait():
	case <-ctx.Done():
	}
	return
}

func (c *DefaultDialerClient) PostPacket(ctx context.Context, url string, body io.Reader, contentLength int64) error {
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return err
	}
	req.ContentLength = contentLength
	req.Header = c.options.GetRequestHeader(url)
	if c.httpVersion != "1.1" {
		resp, err := c.client.Do(req)
		if err != nil {
			c.closed.Store(true)
			return err
		}
		io.Copy(io.Discard, resp.Body)
		defer resp.Body.Close()
	} else {
		// Keep one upload request in flight per raw HTTP/1.1 connection.
		// Before reusing a pooled connection, fully consume its previous
		// response; after writing the current request, return immediately so
		// packet-up startup does not wait for a response that may depend on the
		// download side making progress.
		requestBuff := new(bytes.Buffer)
		common.Must(req.Write(requestBuff))
		for {
			h1UploadConn := c.uploadRawPool.Get()
			newConnection := h1UploadConn == nil
			if newConnection {
				newConn, err := c.dialUploadConn(ctx)
				if err != nil {
					return err
				}
				h1UploadConn = NewH1Conn(newConn)
			}

			// Raw HTTP/1.1 connections are outside net/http, so context
			// cancellation must interrupt both response reads and writes.
			cancelDone := make(chan struct{})
			stopCancel := context.AfterFunc(ctx, func() {
				_ = h1UploadConn.SetDeadline(time.Now())
				close(cancelDone)
			})
			clearDeadline := func() {
				if !stopCancel() {
					<-cancelDone
				}
				_ = h1UploadConn.SetDeadline(time.Time{})
			}

			connectionClosed := false
			for h1UploadConn.PendingResponses > 0 {
				resp, readErr := http.ReadResponse(h1UploadConn.RespBufReader, req)
				if readErr != nil {
					clearDeadline()
					_ = h1UploadConn.Close()
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return fmt.Errorf("error while reading response: %w", readErr)
				}
				h1UploadConn.PendingResponses--
				_, copyErr := io.Copy(io.Discard, resp.Body)
				closeErr := resp.Body.Close()
				if copyErr != nil || closeErr != nil {
					clearDeadline()
					_ = h1UploadConn.Close()
					if copyErr != nil {
						return fmt.Errorf("error while draining response: %w", copyErr)
					}
					return fmt.Errorf("error while closing response: %w", closeErr)
				}
				if resp.StatusCode != http.StatusOK {
					clearDeadline()
					_ = h1UploadConn.Close()
					return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
				}
				if resp.Close {
					clearDeadline()
					_ = h1UploadConn.Close()
					connectionClosed = true
					break
				}
			}
			if connectionClosed {
				continue
			}

			_, writeErr := h1UploadConn.Write(requestBuff.Bytes())
			if writeErr != nil {
				clearDeadline()
				_ = h1UploadConn.Close()
				// A pooled keep-alive connection may have been closed by the
				// peer while idle. Retry once a newly dialed connection is used.
				if !newConnection {
					continue
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return writeErr
			}
			h1UploadConn.PendingResponses++
			clearDeadline()
			if req.Close {
				_ = h1UploadConn.Close()
			} else {
				c.uploadRawPool.Put(h1UploadConn)
			}
			break
		}
	}

	return nil
}

type WaitReadCloser struct {
	ctx  context.Context
	Wait chan struct{}
	io.ReadCloser
}

func (w *WaitReadCloser) Set(rc io.ReadCloser) {
	w.ReadCloser = rc
	defer func() {
		if recover() != nil {
			rc.Close()
		}
	}()
	close(w.Wait)
}

func (w *WaitReadCloser) Read(b []byte) (int, error) {
	select {
	case <-w.ctx.Done():
		return 0, w.ctx.Err()
	default:
	}

	if w.ReadCloser == nil {
		select {
		case <-w.ctx.Done():
			return 0, w.ctx.Err()
		case <-w.Wait:
		}
		if w.ReadCloser == nil {
			return 0, io.ErrClosedPipe
		}
	}
	return w.ReadCloser.Read(b)
}

func (w *WaitReadCloser) Close() error {
	if w.ReadCloser != nil {
		return w.ReadCloser.Close()
	}
	defer func() {
		if recover() != nil && w.ReadCloser != nil {
			w.ReadCloser.Close()
		}
	}()
	close(w.Wait)
	return nil
}
