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

package http2_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	nativehttp "github.com/nukilabs/http"
	nativeh2 "github.com/nukilabs/http/http2"
	nativetls "github.com/nukilabs/utls"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type observation struct {
	window    uint32
	written   int
	processed bool
	err       error
}

func peer(t *testing.T, size int) (string, <-chan observation) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	completed := make(chan observation, 1)
	go func() {
		observed := observation{window: 65535}
		defer func() { completed <- observed }()
		conn, err := listener.Accept()
		if err != nil {
			observed.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		preface := make([]byte, len(peerh2.ClientPreface))
		if _, err = io.ReadFull(conn, preface); err != nil || string(preface) != peerh2.ClientPreface {
			observed.err = errors.Join(err, errors.New("invalid client preface"))
			return
		}
		framer := peerh2.NewFramer(conn, conn)
		if err = framer.WriteSettings(); err != nil {
			observed.err = err
			return
		}
		var stream uint32
		for stream == 0 {
			frame, err := framer.ReadFrame()
			if err != nil {
				observed.err = err
				return
			}
			switch value := frame.(type) {
			case *peerh2.SettingsFrame:
				if !value.IsAck() {
					_ = value.ForeachSetting(func(setting peerh2.Setting) error {
						if setting.ID == peerh2.SettingInitialWindowSize {
							observed.window = setting.Val
						}
						return nil
					})
					_ = framer.WriteSettingsAck()
				}
			case *peerh2.HeadersFrame:
				stream = value.StreamID
			}
		}
		var encoded bytes.Buffer
		encoder := hpack.NewEncoder(&encoded)
		_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(size)})
		if err = framer.WriteHeaders(peerh2.HeadersFrameParam{StreamID: stream, EndHeaders: true, EndStream: size == 0, BlockFragment: encoded.Bytes()}); err != nil {
			observed.err = err
			return
		}
		data := bytes.Repeat([]byte{'x'}, 16384)
		for remaining := size; remaining > 0; {
			count := min(remaining, len(data))
			if err = framer.WriteData(stream, remaining == count, data[:count]); err != nil {
				observed.err = err
				return
			}
			observed.written += count
			remaining -= count
		}
		ping := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
		if err = framer.WritePing(false, ping); err != nil {
			observed.err = err
			return
		}
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				observed.err = err
				return
			}
			if ack, ok := frame.(*peerh2.PingFrame); ok && ack.IsAck() && ack.Data == ping {
				observed.processed = true
				return
			}
			if away, ok := frame.(*peerh2.GoAwayFrame); ok {
				observed.err = fmt.Errorf("peer GOAWAY %s", away.ErrCode)
				return
			}
		}
	}()
	t.Cleanup(func() { _ = listener.Close() })
	return "http://" + listener.Addr().String(), completed
}

func TestFathomrySelectedHTTP2WireReceiveWindow(t *testing.T) {
	for _, test := range []struct {
		name      string
		window    *uint32
		config    int
		size      int
		reject    bool
		generated bool
	}{
		{"native-six-mib-small", ptr(uint32(6 << 20)), 0, 3 << 20, false, false},
		{"native-six-mib-five-mib", ptr(uint32(6 << 20)), 0, 5 << 20, false, false},
		{"public-config-six-mib", ptr(uint32(6 << 20)), 6 << 20, 5 << 20, false, false},
		{"zero-empty-control", ptr(uint32(0)), 0, 0, false, false},
		{"zero-default", ptr(uint32(0)), 0, 1, true, false},
		{"zero-negative", ptr(uint32(0)), -1, 1, true, false},
		{"zero-smallest-positive", ptr(uint32(0)), 1, 1, true, false},
		{"omitted-one-over", nil, 0, 65536, true, false},
		{"public-config-omitted", nil, 65535, 65536, true, false},
		{"generated-default", nil, 0, 3 << 20, false, true},
		{"generated-configured", nil, 6 << 20, 5 << 20, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint, done := peer(t, test.size)
			parent := &nativehttp.Transport{HTTP2: &nativehttp.HTTP2Config{MaxReceiveBufferPerStream: test.config}}
			transport, err := nativeh2.ConfigureTransports(parent)
			if err != nil {
				t.Fatal(err)
			}
			transport.AllowHTTP = true
			transport.DialTLSContext = func(ctx context.Context, network, address string, _ *nativetls.Config) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, address)
			}
			if !test.generated {
				transport.Settings = []nativeh2.Setting{{ID: nativeh2.SettingEnablePush, Val: 0}}
				if test.window != nil {
					transport.Settings = append(transport.Settings, nativeh2.Setting{ID: nativeh2.SettingInitialWindowSize, Val: *test.window})
				}
			}
			defer transport.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			request, _ := nativehttp.NewRequestWithContext(ctx, "GET", endpoint, nil)
			location, err := net.Dial("tcp", request.URL.Host)
			if err != nil {
				t.Fatal(err)
			}
			connection, err := transport.NewClientConn(location)
			if err != nil {
				location.Close()
				t.Fatal(err)
			}
			defer connection.Close()
			response, err := connection.RoundTrip(request)
			if err != nil {
				t.Fatal("headers", err)
			}
			defer response.Body.Close()
			var observed observation
			select {
			case observed = <-done:
			case <-ctx.Done():
				t.Fatal("peer did not complete before app read")
			}
			data, readErr := io.ReadAll(response.Body)
			wantWindow := uint32(65535)
			if test.window != nil {
				wantWindow = *test.window
			}
			if test.generated {
				wantWindow = 4 << 20
				if test.config > 0 {
					wantWindow = uint32(test.config)
				}
			}
			if observed.window != wantWindow {
				t.Fatal("wrong wire setting", observed.window, wantWindow)
			}
			t.Logf("wire=%d public_config=%d sent=%d received=%d processed_before_app_read=%v peer_error=%v body_error=%v", observed.window, test.config, observed.written, len(data), observed.processed, observed.err, readErr)
			if test.reject {
				if readErr == nil || observed.processed {
					t.Fatal("accepted DATA beyond advertised receive window")
				}
			} else if readErr != nil || !observed.processed || observed.err != nil || len(data) != test.size {
				t.Fatal("rejected legal DATA within advertised receive window")
			}
		})
	}
}
func ptr[T any](value T) *T { return &value }

func TestFathomryInvalidSettingsCloseTransferredConnection(t *testing.T) {
	for _, setting := range []nativeh2.Setting{{ID: nativeh2.SettingInitialWindowSize, Val: 1 << 31}, {ID: nativeh2.SettingMaxFrameSize, Val: 16383}} {
		local, remote := net.Pipe()
		transport := &nativeh2.Transport{Settings: []nativeh2.Setting{setting}}
		connection, err := transport.NewClientConn(local)
		if err == nil || connection != nil {
			local.Close()
			remote.Close()
			t.Fatal("invalid native settings installed")
		}
		_ = remote.SetReadDeadline(time.Now().Add(time.Second))
		var data [1]byte
		count, readErr := remote.Read(data[:])
		_ = remote.Close()
		if count != 0 || !errors.Is(readErr, io.EOF) {
			t.Fatal("rejected connection retained socket or wrote bytes", count, readErr)
		}
	}
}
