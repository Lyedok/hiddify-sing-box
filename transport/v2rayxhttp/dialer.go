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
	uploadRawPool  *sync.Pool
	dialUploadConn func(ctxInner context.Context) (net.Conn, error)
}

func (c *DefaultDialerClient) IsClosed() bool {
	return c.closed.Load()
}

func (c *DefaultDialerClient) CloseIdleConnections() {
	c.client.CloseIdleConnections()
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
		// Stringify the entire HTTP/1.1 request so it can be safely retried.
		// Calling req.Write multiple times would reuse an already drained body.
		requestBuff := new(bytes.Buffer)
		common.Must(req.Write(requestBuff))
		for {
			uploadConn := c.uploadRawPool.Get()
			newConnection := uploadConn == nil
			var h1UploadConn *H1Conn
			if newConnection {
				newConn, err := c.dialUploadConn(ctx)
				if err != nil {
					return err
				}
				h1UploadConn = NewH1Conn(newConn)
			} else {
				h1UploadConn = uploadConn.(*H1Conn)
			}

			// Raw HTTP/1.1 connections are outside net/http, so context
			// cancellation must interrupt both the write and response read.
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

			resp, readErr := http.ReadResponse(h1UploadConn.RespBufReader, req)
			if readErr != nil {
				clearDeadline()
				_ = h1UploadConn.Close()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("error while reading response: %w", readErr)
			}
			_, copyErr := io.Copy(io.Discard, resp.Body)
			closeErr := resp.Body.Close()
			clearDeadline()
			if copyErr != nil || closeErr != nil {
				_ = h1UploadConn.Close()
				if copyErr != nil {
					return fmt.Errorf("error while draining response: %w", copyErr)
				}
				return fmt.Errorf("error while closing response: %w", closeErr)
			}
			if resp.StatusCode != http.StatusOK {
				_ = h1UploadConn.Close()
				return fmt.Errorf("got non-200 error response code: %d", resp.StatusCode)
			}
			if resp.Close || req.Close {
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
