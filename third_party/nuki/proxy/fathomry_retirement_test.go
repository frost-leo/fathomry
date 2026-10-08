// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nukilabs/quic-go"
	"github.com/nukilabs/quic-go/http3"
	"github.com/nukilabs/quic-go/qlogwriter"
	utls "github.com/nukilabs/utls"
	official "github.com/quic-go/quic-go"
)

type retainedControlTrace struct {
	entered, release, exited chan struct{}
	once                     sync.Once
}

func (trace *retainedControlTrace) SupportsSchemas(string) bool { return true }
func (trace *retainedControlTrace) AddProducer() qlogwriter.Recorder {
	return retainedControlRecorder{trace}
}

type retainedControlRecorder struct{ trace *retainedControlTrace }

func (recorder retainedControlRecorder) RecordEvent(event qlogwriter.Event) {
	if event.Name() == "http3:frame_parsed" {
		recorder.trace.once.Do(func() {
			close(recorder.trace.entered)
			<-recorder.trace.release
			close(recorder.trace.exited)
		})
	}
}
func (retainedControlRecorder) Close() error { return nil }

type replacementProbe struct{ sockets atomic.Int32 }

var errReplacementProbe = errors.New("replacement reached controlled socket acquisition")

func (*replacementProbe) SupportHTTP3() bool { return true }
func (*replacementProbe) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errReplacementProbe
}
func (probe *replacementProbe) ListenPacket(context.Context, string, string) (net.PacketConn, error) {
	probe.sockets.Add(1)
	return nil, errReplacementProbe
}

func TestFathomryMASQUEReplacementJoinsOldHTTP3Control(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	certificate := httptest.NewTLSServer(nil)
	certificates := certificate.TLS.Certificates
	roots := x509.NewCertPool()
	roots.AddCert(certificate.Certificate())
	certificate.Close()
	listener, err := official.ListenAddr("127.0.0.1:0", &tls.Config{Certificates: certificates, NextProtos: []string{"h3"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peers := make(chan *official.Conn, 1)
	peerErrors := make(chan error, 1)
	go func() {
		peer, err := listener.Accept(ctx)
		if err != nil {
			peerErrors <- err
			return
		}
		peers <- peer
	}()
	trace := &retainedControlTrace{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
	release := sync.OnceFunc(func() { close(trace.release) })
	defer release()
	connection, err := quic.DialAddr(ctx, listener.Addr().String(), &utls.Config{RootCAs: roots, NextProtos: []string{"h3"}}, &quic.Config{
		Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return trace },
	})
	if err != nil {
		t.Fatal(err)
	}
	client := (&http3.Transport{}).NewClientConn(connection)
	t.Cleanup(func() { release(); _ = client.CloseWithError(0, "") })
	var peer *official.Conn
	select {
	case peer = <-peers:
	case err := <-peerErrors:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(context.Cause(ctx))
	}
	defer peer.CloseWithError(0, "")
	control, err := peer.OpenUniStreamSync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Write([]byte{0, 4, 0}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-trace.entered:
	case <-ctx.Done():
		t.Fatal("old HTTP3 control worker did not enter callback")
	}
	_ = peer.CloseWithError(0, "")
	select {
	case <-connection.Context().Done():
	case <-ctx.Done():
		t.Fatal("raw old QUIC did not terminate")
	}
	address, _ := url.Parse("https://" + listener.Addr().String() + "/udp/{target_host}/{target_port}/")
	base := &replacementProbe{}
	selected, err := NewWithDialer(address, time.Second, &utls.Config{RootCAs: roots}, base, nil, 65536)
	if err != nil {
		t.Fatal(err)
	}
	dialer := selected.(*Dialer)
	dialer.h3Conn, dialer.h3ClientConn = connection, client
	t.Cleanup(func() { release(); _ = dialer.Close() })
	done := make(chan error, 1)
	go func() { _, err := dialer.ListenPacket(ctx, "udp", "127.0.0.1:443"); done <- err }()
	select {
	case err := <-done:
		t.Fatal("replacement discarded an unjoined HTTP3 owner", err)
	case <-time.After(20 * time.Millisecond):
	}
	if base.sockets.Load() != 0 {
		t.Fatal("replacement acquired resources before old control-worker release")
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, errReplacementProbe) || base.sockets.Load() != 1 {
			t.Fatal("replacement did not resume after joined retirement", err, base.sockets.Load())
		}
	case <-ctx.Done():
		t.Fatal("old owner release did not unblock replacement")
	}
	select {
	case <-trace.exited:
	default:
		t.Fatal("replacement resumed before native callback exit")
	}
}
