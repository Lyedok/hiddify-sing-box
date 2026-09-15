//go:build with_utls

package tls

import (
	"crypto/ecdh"
	"crypto/rand"
	"testing"

	utls "github.com/metacubex/utls"
)

func TestRealityECDHEKeySupportsMLKEMChromeHello(t *testing.T) {
	if realityClientVersion != [3]byte{26, 9, 9} {
		t.Fatalf("unexpected REALITY policy version: %v", realityClientVersion)
	}
	standard, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	mlkem, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	if got := realityECDHEKey(nil); got != nil {
		t.Fatal("nil key-share state returned a key")
	}
	if got := realityECDHEKey(&utls.KeySharePrivateKeys{MlkemEcdhe: mlkem}); got != mlkem {
		t.Fatal("ML-KEM X25519 component was not selected")
	}
	if got := realityECDHEKey(&utls.KeySharePrivateKeys{Ecdhe: standard, MlkemEcdhe: mlkem}); got != standard {
		t.Fatal("standard ECDHE key did not take precedence")
	}
}
