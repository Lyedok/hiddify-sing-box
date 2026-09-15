package dialer

import (
	"context"
	"net"
	"testing"
	"time"

	tf "github.com/sagernet/sing-box/common/tlsfragment"
	"github.com/sagernet/sing-box/option"
	N "github.com/sagernet/sing/common/network"
)

func TestDefaultDialerEnablesOutboundClientHelloFragment(t *testing.T) {
	dialer, err := NewDefault(context.Background(), option.DialerOptions{
		TCPFastOpen: true,
		TLSFragment: option.TLSFragmentOptions{
			Enabled: true,
			Method:  "tlsHello",
			Size:    "100-200",
			Sleep:   "10-20",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if dialer.tlsFragment == nil {
		t.Fatal("TLS fragment config was not attached to the outbound dialer")
	}
	if dialer.tlsFragment.LengthMin != 100 || dialer.tlsFragment.LengthMax != 200 {
		t.Fatalf("unexpected fragment range: %+v", dialer.tlsFragment)
	}
	if dialer.tlsFragment.IntervalMin != 10*time.Millisecond || dialer.tlsFragment.IntervalMax != 20*time.Millisecond {
		t.Fatalf("unexpected sleep range: %+v", dialer.tlsFragment)
	}
	if !dialer.dialer4.DisableTFO || !dialer.dialer6.DisableTFO {
		t.Fatal("TCP Fast Open remained enabled with ClientHello fragmentation")
	}

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	wrapped := dialer.wrapTLSFragment(context.Background(), N.NetworkTCP, client)
	if _, ok := wrapped.(*tf.ClientHelloConn); !ok {
		t.Fatalf("TCP connection was not wrapped: %T", wrapped)
	}
	if got := dialer.wrapTLSFragment(context.Background(), N.NetworkUDP, client); got != client {
		t.Fatalf("UDP connection was wrapped: %T", got)
	}
}

func TestDefaultDialerRejectsInvalidOutboundFragment(t *testing.T) {
	for _, fragment := range []option.TLSFragmentOptions{
		{Enabled: true, Method: "range", Size: "100-200", Sleep: "10-20"},
		{Enabled: true, Method: "tlsHello", Size: "zero", Sleep: "10-20"},
		{Enabled: true, Method: "tlsHello", Size: "100-200", Sleep: "bad"},
	} {
		if _, err := NewDefault(context.Background(), option.DialerOptions{TLSFragment: fragment}); err == nil {
			t.Fatalf("accepted invalid TLS fragment config: %+v", fragment)
		}
	}
}
