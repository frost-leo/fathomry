/**
 * fathomry
 * Copyright (C) 2026  Frost Leo
 * SPDX-License-Identifier: GPL-3.0-or-later
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package surf

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	stdhttp "net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	nativeh2 "github.com/enetx/http2"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	"github.com/frost-leo/fathomry/internal/fault"
	utls "github.com/refraction-networking/utls"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type reviewHelloConn struct {
	net.Conn
	writes, closes *atomic.Int64
}

func (conn *reviewHelloConn) Write(data []byte) (int, error) {
	conn.writes.Add(1)
	return conn.Conn.Write(data)
}

func (conn *reviewHelloConn) Close() error {
	conn.closes.Add(1)
	return conn.Conn.Close()
}

func TestLazyHelloOutputBudget(t *testing.T) {
	for _, selection := range []string{"small-control", "encoded-bytes", "extension-count", "cipher-count", "compression-count"} {
		t.Run(selection, func(t *testing.T) {
			var factories, dials, writes, closes, hellos atomic.Int64
			peer := newPeers(t, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
				_, _ = io.WriteString(writer, "hello-control")
			}, false, &tls.Config{GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
				if slices.Contains(hello.Extensions, uint16(0xffb7)) {
					hellos.Add(1)
				}
				return nil, nil
			}})
			factory := func(context.Context) (utls.ClientHelloSpec, error) {
				factories.Add(1)
				spec, err := utls.UTLSIdToSpec(utls.HelloChrome_Auto)
				if err != nil {
					return spec, err
				}
				extension := &utls.GenericExtension{Id: 0xffb7, Data: []byte("bounded")}
				spec.Extensions = append(spec.Extensions, extension)
				switch selection {
				case "encoded-bytes":
					extension.Data = make([]byte, 8192)
				case "extension-count":
					for len(spec.Extensions) <= 256 {
						spec.Extensions = append(spec.Extensions, &utls.GenericExtension{Id: 0xffb8})
					}
				case "cipher-count":
					spec.CipherSuites = make([]uint16, 513)
				case "compression-count":
					spec.CompressionMethods = make([]byte, 257)
				}
				return spec, nil
			}
			options := OptionsV1{Name: "lazy-hello", Mode: HTTP1Only, MaxProfileBytes: 8192,
				Native: NativeOptionsV1{TLSConfig: &tls.Config{RootCAs: peer.roots}, HelloSpecFactory: factory,
					DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
						dials.Add(1)
						conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
						if err != nil {
							return nil, err
						}
						return &reviewHelloConn{Conn: conn, writes: &writes, closes: &closes}, nil
					}}}
			fixture := newFixture(t, options, 1)
			if factories.Load() != 0 || dials.Load() != 0 {
				t.Fatal("offline preparation/construction invoked borrowed dependencies")
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: selection}, request(t, "GET", peer.tcp.URL, ""))
			if receipt == nil {
				t.Fatal("admitted factory produced no independent receipt", err)
			}
			result := settle(t, fixture, receipt)
			if selection == "small-control" {
				if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "hello-control" || hellos.Load() != 1 || writes.Load() == 0 {
					t.Fatal("valid Hello did not reach independent TLS peer", err, result.Err())
				}
			} else if !errors.Is(err, ErrLimit) || !errors.Is(err, sdk.ErrFathomryProfileLimit) || !errors.Is(result.Err(), sdk.ErrFathomryProfileLimit) || result.Outcome.Value.Complete() || writes.Load() != 0 || hellos.Load() != 0 {
				t.Fatal("oversized Hello escaped owned pre-wire guard", err, result.Err(), writes.Load(), hellos.Load())
			}
			if err := fixture.assembly.Close(testContext(t)); err != nil {
				t.Fatal(err)
			}
			if factories.Load() != 1 || dials.Load() != 1 || closes.Load() != 1 {
				t.Fatal("Hello callback/socket ownership changed", factories.Load(), dials.Load(), closes.Load())
			}
		})
	}
}

func reviewMinimalVariant() profiles.Variant {
	profile := chrome.Desktop
	profile.HelloSpec, profile.HelloID, profile.ShuffleExtensions = nil, utls.ClientHelloID{}, false
	profile.ConfigureH2 = func(profiles.H2Config) {}
	profile.ConfigureH3 = func(profiles.H3Config) {}
	profile.BuildHeaders = func(profiles.OSKey) *g.MapOrd[g.String, g.String] {
		headers := g.NewMapOrd[g.String, g.String]()
		return &headers
	}
	return profile
}

type reviewPriorityObservation struct {
	priorities int
	err        error
}

func reviewPriorityPeer(t *testing.T) (string, <-chan reviewPriorityObservation) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	observations := make(chan reviewPriorityObservation, 1)
	go func() {
		defer close(finished)
		observed := reviewPriorityObservation{}
		defer func() { observations <- observed }()
		conn, err := listener.Accept()
		if err != nil {
			observed.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		preface := make([]byte, len(peerh2.ClientPreface))
		if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != peerh2.ClientPreface {
			observed.err = fmt.Errorf("invalid H2 preface: %w", err)
			return
		}
		framer := peerh2.NewFramer(conn, conn)
		if observed.err = framer.WriteSettings(); observed.err != nil {
			return
		}
		var stream uint32
		var requestEnded, settingsAcknowledged bool
		// Do not close this one-shot peer while request or SETTINGS writes remain.
		for stream == 0 || !requestEnded || !settingsAcknowledged {
			frame, err := framer.ReadFrame()
			if err != nil {
				observed.err = err
				return
			}
			switch frame := frame.(type) {
			case *peerh2.PriorityFrame:
				observed.priorities++
				if frame.StreamID != uint32(2*observed.priorities+1) || frame.Weight != 15 {
					observed.err = errors.New("native priority frame data changed")
					return
				}
			case *peerh2.SettingsFrame:
				if frame.IsAck() {
					settingsAcknowledged = true
				} else {
					if observed.err = framer.WriteSettingsAck(); observed.err != nil {
						return
					}
				}
			case *peerh2.HeadersFrame:
				stream = frame.StreamID
				requestEnded = frame.StreamEnded()
			case *peerh2.DataFrame:
				if stream == 0 || frame.StreamID != stream {
					observed.err = errors.New("request DATA used unexpected stream")
					return
				}
				requestEnded = frame.StreamEnded()
			}
		}
		var block bytes.Buffer
		encoder := hpack.NewEncoder(&block)
		_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "2"})
		if observed.err = framer.WriteHeaders(peerh2.HeadersFrameParam{StreamID: stream, EndHeaders: true, BlockFragment: block.Bytes()}); observed.err != nil {
			return
		}
		observed.err = framer.WriteData(stream, true, []byte("ok"))
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-finished:
		case <-time.After(4 * time.Second):
			t.Error("priority peer did not terminate")
		}
	})
	return "http://" + listener.Addr().String(), observations
}

func TestLazyH2PriorityOutputBudget(t *testing.T) {
	for _, count := range []int{24, 25} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			var callbacks, dials atomic.Int64
			endpoint, observations := reviewPriorityPeer(t)
			profile := reviewMinimalVariant()
			profile.ConfigureH2 = func(config profiles.H2Config) {
				callbacks.Add(1)
				frames := make([]nativeh2.PriorityFrame, count)
				for index := range frames {
					frames[index] = nativeh2.PriorityFrame{FrameHeader: nativeh2.FrameHeader{StreamID: uint32(2*index + 3)}, PriorityParam: nativeh2.PriorityParam{Weight: 15}}
				}
				config.PriorityFrames(frames)
			}
			fixture := newFixture(t, OptionsV1{Name: "lazy-priority", Mode: H2C, MaxProfileBytes: 1024,
				Native: NativeOptionsV1{Profile: &profile, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}}, 1)
			if callbacks.Load() != 0 || dials.Load() != 0 {
				t.Fatal("offline selection invoked lazy H2 output")
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "priority"}, request(t, "GET", endpoint, ""))
			result := settle(t, fixture, receipt)
			if callbacks.Load() != 1 {
				t.Fatal("H2 lazy callback count changed", callbacks.Load())
			}
			if count == 25 {
				if !errors.Is(err, sdk.ErrFathomryProfileLimit) || !errors.Is(result.Err(), sdk.ErrFathomryProfileLimit) || dials.Load() != 0 || result.Outcome.Value.Complete() {
					t.Fatal("over-budget priorities installed or dialed", err, result.Err(), dials.Load())
				}
				return
			}
			if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().Protocol() != "HTTP/2.0" || string(result.Outcome.Value.DataCopy()) != "ok" || dials.Load() != 1 {
				t.Fatal("exact-declaration H2 priority control failed", err, result.Err())
			}
			select {
			case observed := <-observations:
				if observed.err != nil || observed.priorities != count {
					t.Fatal("native priorities did not appear on wire", observed.priorities, observed.err)
				}
			case <-testContext(t).Done():
				t.Fatal("priority peer observation unavailable")
			}
		})
	}
}

func TestLazyH3OutputBudget(t *testing.T) {
	for _, selection := range []string{"small-control", "oversized-settings", "max-varint-control", "unrepresentable-varint"} {
		t.Run(selection, func(t *testing.T) {
			var callbacks, packets, requests atomic.Int64
			peer := newPeers(t, func(writer stdhttp.ResponseWriter, request *stdhttp.Request) {
				requests.Add(1)
				_, _ = io.WriteString(writer, request.Proto)
			}, true)
			profile := reviewMinimalVariant()
			profile.ConfigureH3 = func(config profiles.H3Config) {
				callbacks.Add(1)
				switch selection {
				case "small-control":
					for range 16 {
						config.Grease()
					}
				case "oversized-settings":
					for range 64 {
						config.Grease()
					}
				case "max-varint-control":
					config.MaxFieldSectionSize(1<<62 - 1)
				case "unrepresentable-varint":
					config.MaxFieldSectionSize(1 << 62)
				}
			}
			fixture := newFixture(t, OptionsV1{Name: "lazy-h3", Mode: PreferHTTP3, MaxProfileBytes: 1024,
				Native: NativeOptionsV1{Profile: &profile, TLSConfig: &tls.Config{RootCAs: peer.roots},
					ListenPacket: func(ctx context.Context, network, address string) (net.PacketConn, error) {
						packets.Add(1)
						return (&net.ListenConfig{}).ListenPacket(ctx, network, address)
					}}}, 1)
			if callbacks.Load() != 0 || packets.Load() != 0 {
				t.Fatal("offline selection invoked lazy H3 output")
			}
			receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: selection}, request(t, "GET", peer.tcp.URL, ""))
			result := settle(t, fixture, receipt)
			if callbacks.Load() != 1 {
				t.Fatal("H3 lazy callback count changed", callbacks.Load())
			}
			if selection == "oversized-settings" || selection == "unrepresentable-varint" {
				if !errors.Is(err, sdk.ErrFathomryProfileLimit) || !errors.Is(result.Err(), sdk.ErrFathomryProfileLimit) || packets.Load() != 0 || requests.Load() != 0 || result.Outcome.Value.Complete() {
					t.Fatal("over-budget H3 settings installed or acquired packet", err, result.Err(), packets.Load(), requests.Load())
				}
			} else if err != nil || result.Err() != nil || !result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "HTTP/3.0" || result.Outcome.Value.Metadata().Protocol() != "HTTP/3.0" || packets.Load() != 1 || requests.Load() != 1 {
				t.Fatal("valid H3 profile control did not reach H3 peer", err, result.Err(), packets.Load(), requests.Load())
			}
		})
	}
}
