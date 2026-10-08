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
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enetx/g"
	http "github.com/enetx/http"
	nativeh2 "github.com/enetx/http2"
	sdk "github.com/enetx/surf"
	"github.com/enetx/surf/profiles"
	"github.com/enetx/surf/profiles/chrome"
	utls "github.com/refraction-networking/utls"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type independentH2Result struct {
	window    uint32
	written   int
	processed bool
	err       error
}

func independentH2Peer(t *testing.T, size int) (string, <-chan independentH2Result) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	finished := make(chan independentH2Result, 1)
	go func() {
		result := independentH2Result{window: 65535}
		defer func() { finished <- result }()
		conn, err := listener.Accept()
		if err != nil {
			result.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		preface := make([]byte, len(peerh2.ClientPreface))
		if _, err := io.ReadFull(conn, preface); err != nil {
			result.err = err
			return
		}
		if string(preface) != peerh2.ClientPreface {
			result.err = errors.New("not prior-knowledge H2")
			return
		}
		framer := peerh2.NewFramer(conn, conn)
		if err := framer.WriteSettings(); err != nil {
			result.err = err
			return
		}
		var stream uint32
		for stream == 0 {
			frame, err := framer.ReadFrame()
			if err != nil {
				result.err = err
				return
			}
			switch frame := frame.(type) {
			case *peerh2.SettingsFrame:
				if !frame.IsAck() {
					if setting, ok := frame.Value(peerh2.SettingInitialWindowSize); ok {
						result.window = setting
					}
					if err := framer.WriteSettingsAck(); err != nil {
						result.err = err
						return
					}
				}
			case *peerh2.HeadersFrame:
				stream = frame.StreamID
			}
		}
		var block bytes.Buffer
		encoder := hpack.NewEncoder(&block)
		_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(size)})
		if err := framer.WriteHeaders(peerh2.HeadersFrameParam{StreamID: stream, EndHeaders: true, BlockFragment: block.Bytes()}); err != nil {
			result.err = err
			return
		}
		data := bytes.Repeat([]byte("z"), 16<<10)
		for result.written < size {
			count := min(len(data), size-result.written)
			if err := framer.WriteData(stream, result.written+count == size, data[:count]); err != nil {
				result.err = err
				return
			}
			result.written += count
		}
		probe := [8]byte{'p', 'r', 'o', 'c', 'e', 's', 's', 'd'}
		if err := framer.WritePing(false, probe); err != nil {
			result.err = err
			return
		}
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				result.err = err
				return
			}
			switch frame := frame.(type) {
			case *peerh2.PingFrame:
				if frame.IsAck() && frame.Data == probe {
					result.processed = true
					return
				}
			case *peerh2.GoAwayFrame:
				result.err = fmt.Errorf("peer received GOAWAY: %s", frame.ErrCode)
				return
			}
		}
	}()
	return "http://" + listener.Addr().String(), finished
}

func TestNativeH2AdvertisedReceiveWindow(t *testing.T) {
	for _, size := range []int{3 << 20, 5 << 20} {
		t.Run(fmt.Sprintf("advertised6MiB-payload%dMiB", size>>20), func(t *testing.T) {
			endpoint, peer := independentH2Peer(t, size)
			transport := &nativeh2.Transport{AllowHTTP: true, Settings: []nativeh2.Setting{{ID: nativeh2.SettingEnablePush, Val: 0}, {ID: nativeh2.SettingInitialWindowSize, Val: 6 << 20}},
				DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, network, address)
				}}
			defer transport.CloseIdleConnections()
			request, err := http.NewRequestWithContext(testContext(t), "GET", endpoint, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var sent independentH2Result
			select {
			case sent = <-peer:
			case <-testContext(t).Done():
				t.Fatal("peer could not finish sending before application reads")
			}
			if sent.window != 6<<20 {
				t.Fatal("peer did not receive selected window", sent.window)
			}
			data, readErr := io.ReadAll(response.Body)
			t.Logf("advertised=%d peer-written=%d processed-before-app-read=%t app-read=%d peer-error=%v body-error=%v", sent.window, sent.written, sent.processed, len(data), sent.err, readErr)
			if readErr != nil || len(data) != size || sent.err != nil || !sent.processed || !bytes.Equal(data, bytes.Repeat([]byte("z"), size)) {
				t.Fatal("valid response within advertised window was rejected before application consumption")
			}
		})
	}
}

func TestH2CManagedDialControls(t *testing.T) {
	for _, mode := range []string{"h1-refusal-control", "h2c-refusal", "h2c-owned"} {
		t.Run(mode, func(t *testing.T) {
			var acquisitions, dials, releases, requests atomic.Int64
			refused := errors.New("synthetic managed capacity refusal")
			var endpoint string
			if mode == "h1-refusal-control" {
				peer := httptest.NewServer(stdhttp.HandlerFunc(func(writer stdhttp.ResponseWriter, _ *stdhttp.Request) {
					requests.Add(1)
					_, _ = io.WriteString(writer, "z")
				}))
				defer peer.Close()
				endpoint = peer.URL
			} else {
				endpoint, _ = independentH2Peer(t, 1)
			}
			client := sdk.NewClient()
			defer client.Close()
			if err := client.ConfigureFathomry(sdk.FathomryControlV1{
				AcquireTCP: func() (func(), error) {
					acquisitions.Add(1)
					if mode != "h2c-owned" {
						return nil, refused
					}
					return func() { releases.Add(1) }, nil
				},
				DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
					dials.Add(1)
					return (&net.Dialer{}).DialContext(ctx, network, address)
				},
			}); err != nil {
				t.Fatal(err)
			}
			builder := client.Builder().Proxy("")
			if mode != "h1-refusal-control" {
				builder.H2C()
			}
			if result := builder.Build(); result.IsErr() {
				t.Fatal(result.Err())
			}
			result := client.Get(g.String(endpoint)).WithContext(testContext(t)).Do()
			if result.IsOk() {
				response := result.Ok()
				data, err := io.ReadAll(response.Body.Reader)
				if err != nil || string(data) != "z" {
					t.Fatal("normal response failed", err)
				}
				_ = response.Body.Close()
				t.Logf("actual-protocol=%s", response.Proto)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			t.Logf("acquisitions=%d dials=%d releases=%d request-error=%v", acquisitions.Load(), dials.Load(), releases.Load(), result.Err())
			if mode == "h2c-owned" {
				if result.IsErr() || acquisitions.Load() != 1 || dials.Load() != 1 || releases.Load() != 1 || !client.FathomryQuiescent() {
					t.Fatal("h2c bypassed physical socket authority")
				}
			} else if !errors.Is(result.Err(), refused) || acquisitions.Load() != 1 || dials.Load() != 0 || requests.Load() != 0 {
				t.Fatal("managed refusal did not prevent network dispatch")
			}
		})
	}
}

func TestH2CPlainHTTPVariant(t *testing.T) {
	endpoint, peer := independentH2Peer(t, 1)
	client := sdk.NewClient()
	defer client.Close()
	variant := chrome.Desktop
	variant.HelloSpec = nil
	variant.HelloID = utls.ClientHelloID{}
	variant.ShuffleExtensions = false
	if result := client.Builder().Proxy("").H2C().FathomryApplyVariant(variant, profiles.Linux).Build(); result.IsErr() {
		t.Fatal(result.Err())
	}
	result := client.Get(g.String(endpoint)).WithContext(testContext(t)).Do()
	var observed independentH2Result
	select {
	case observed = <-peer:
	case <-testContext(t).Done():
		t.Fatal("independent peer did not observe request")
	}
	t.Logf("transport=%T peer-error=%v request-error=%v", client.GetClient().Transport, observed.err, result.Err())
	if result.IsErr() {
		t.Fatal("HTTP-only variant replaced h2c with a TLS/JA transport")
	}
	response := result.Ok()
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body.Reader)
	if err != nil || string(data) != "z" || response.Proto != "HTTP/2.0" || observed.window != 6<<20 {
		t.Fatal("HTTP-only variant lost H2 profile semantics", err, observed.window)
	}
}
