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

package tlsclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http/httptest"
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

type heldH3Trace struct {
	entered, released, exited chan struct{}
	once                      sync.Once
}

func (trace *heldH3Trace) SupportsSchemas(string) bool { return true }
func (trace *heldH3Trace) AddProducer() qlogwriter.Recorder {
	return heldH3Recorder{trace}
}

type heldH3Recorder struct{ trace *heldH3Trace }

func (recorder heldH3Recorder) RecordEvent(event qlogwriter.Event) {
	if event.Name() == "http3:frame_parsed" {
		recorder.trace.once.Do(func() {
			close(recorder.trace.entered)
			<-recorder.trace.released
			close(recorder.trace.exited)
		})
	}
}
func (heldH3Recorder) Close() error { return nil }

func TestFathomryRacerRetirementJoinsActualH3ControlWork(t *testing.T) {
	for _, mode := range []string{"acquire", "forget", "prune", "shutdown", "live-forget"} {
		t.Run(mode, func(t *testing.T) {
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
			peerReady := make(chan *official.Conn, 1)
			peerError := make(chan error, 1)
			go func() {
				peer, err := listener.Accept(ctx)
				if err != nil {
					peerError <- err
					return
				}
				peerReady <- peer
			}()
			trace := &heldH3Trace{entered: make(chan struct{}), released: make(chan struct{}), exited: make(chan struct{})}
			release := sync.OnceFunc(func() { close(trace.released) })
			defer release()
			transport := &http3.Transport{TLSClientConfig: &utls.Config{RootCAs: roots}, QUICConfig: &quic.Config{
				Tracer: func(context.Context, bool, quic.ConnectionID) qlogwriter.Trace { return trace },
			}}
			var dials atomic.Int32
			racer := newRacer(transport, func(ctx context.Context, address string, config *utls.Config, quicConfig *quic.Config) (*quic.Conn, error) {
				if dials.Add(1) != 1 {
					return nil, errors.New("observed replacement dial")
				}
				return quic.DialAddr(ctx, address, config, quicConfig)
			}, time.Millisecond, true)
			racer.limit = 1
			entry, err := racer.acquire(listener.Addr().String(), true)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				release()
				racer.shutdown()
				if entry.cc != nil {
					_ = entry.cc.CloseWithError(0, "")
				}
			})
			if err := racer.await(ctx, entry, true); err != nil {
				t.Fatal(err)
			}
			var peer *official.Conn
			select {
			case peer = <-peerReady:
			case err := <-peerError:
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
				t.Fatal("HTTP3 control parser did not enter its held callback")
			}
			if mode != "live-forget" && mode != "shutdown" {
				_ = peer.CloseWithError(0, "")
				select {
				case <-entry.cc.Context().Done():
				case <-ctx.Done():
					t.Fatal("raw QUIC connection did not end")
				}
			}
			started, done := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(done)
				close(started)
				switch mode {
				case "acquire":
					_, _ = racer.acquire(listener.Addr().String(), true)
				case "forget", "live-forget":
					racer.forget(listener.Addr().String(), entry.cc)
				case "prune":
					racer.mu.Lock()
					racer.pruneLocked()
					racer.mu.Unlock()
				case "shutdown":
					racer.shutdown()
				}
			}()
			<-started
			if mode == "live-forget" {
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("healthy multiplexed connection was retired")
				}
				kept, err := racer.acquire(listener.Addr().String(), true)
				if err != nil || kept != entry || !entry.live() || dials.Load() != 1 {
					t.Fatal("stream-local failure evicted healthy connection", err, dials.Load())
				}
			} else {
				select {
				case <-done:
					t.Fatal("origin slot or shutdown released before actual HTTP3 control work joined")
				case <-time.After(20 * time.Millisecond):
				}
				if dials.Load() != 1 {
					t.Fatal("replacement exceeded the retained origin resource slot", dials.Load())
				}
			}
			release()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("retirement did not finish after actual callback exit")
			}
			select {
			case <-trace.exited:
			case <-ctx.Done():
				t.Fatal("native callback did not exit")
			}
		})
	}
}
