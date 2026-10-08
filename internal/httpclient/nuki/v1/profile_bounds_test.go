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

package nuki

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	nativetls "github.com/nukilabs/utls"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestProviderH2ConnectionFlowUsesSafeIncrement(t *testing.T) {
	const maximumIncrement uint32 = (1<<31 - 1) - 65535
	for _, flow := range []uint32{0, 1, 65534, 65535, maximumIncrement} {
		certificate := httptest.NewTLSServer(nil)
		defer certificate.Close()
		raw, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listener := tls.NewListener(raw, &tls.Config{Certificates: certificate.TLS.Certificates, NextProtos: []string{"h2"}})
		defer listener.Close()
		type observation struct {
			increment uint32
			err       error
		}
		observed := make(chan observation, 1)
		go func() {
			var result observation
			defer func() { observed <- result }()
			conn, err := listener.Accept()
			if err != nil {
				result.err = err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			preface := make([]byte, len(http2.ClientPreface))
			if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
				result.err = errors.Join(err, errors.New("invalid H2 preface"))
				return
			}
			framer := http2.NewFramer(conn, conn)
			if result.err = framer.WriteSettings(); result.err != nil {
				return
			}
			for {
				frame, err := framer.ReadFrame()
				if err != nil {
					result.err = err
					return
				}
				switch frame := frame.(type) {
				case *http2.SettingsFrame:
					if !frame.IsAck() {
						if result.err = framer.WriteSettingsAck(); result.err != nil {
							return
						}
					}
				case *http2.WindowUpdateFrame:
					if frame.StreamID == 0 {
						result.increment = frame.Increment
					}
				case *http2.HeadersFrame:
					var block bytes.Buffer
					encoder := hpack.NewEncoder(&block)
					_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
					_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "0"})
					result.err = framer.WriteHeaders(http2.HeadersFrameParam{StreamID: frame.StreamID, EndHeaders: true, EndStream: true, BlockFragment: block.Bytes()})
					return
				}
			}
		}()
		options := providerOptions()
		h2 := *options.Native.Profile.H2
		h2.ConnectionFlow = flow
		options.Native.Profile.H2, options.Native.TLS = &h2, testRoots(certificate)
		fixture := bindProvider(t, options)
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "connection-flow"}, nativeRequest(t, "GET", "https://"+raw.Addr().String(), nil))
		if err != nil {
			t.Fatal(err)
		}
		result := outcome(t, fixture, receipt)
		peer := <-observed
		want := flow
		if want < 65535 {
			want = 1 << 30
		}
		if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != "HTTP/2.0" || peer.err != nil || peer.increment != want || uint64(peer.increment)+65535 > 1<<31-1 {
			t.Fatal("native connection flow increment", flow, peer.increment, result.Err(), peer.err)
		}
		t.Logf("profile=%d observed_WINDOW_UPDATE=%d resulting_credit=%d", flow, peer.increment, uint64(peer.increment)+65535)
	}
	options := providerOptions()
	h2 := *options.Native.Profile.H2
	h2.ConnectionFlow = maximumIncrement + 1
	options.Native.Profile.H2 = &h2
	var effects atomic.Int32
	options.Native.Profile.ClientHelloSpec = func() *nativetls.ClientHelloSpec { effects.Add(1); return nil }
	options.Native.DialContext = func(context.Context, string, string) (net.Conn, error) {
		effects.Add(1)
		return nil, errors.New("unexpected socket")
	}
	if _, err := PrepareV1(options); !errors.Is(err, ErrInput) || effects.Load() != 0 {
		t.Fatal("overflowing connection credit reached construction", err, effects.Load())
	}
}
