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
	"strconv"
	"sync"
	"testing"
	"time"

	http "github.com/enetx/http"
	nativeh2 "github.com/enetx/http2"
	peerh2 "golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

type reviewH2Observation struct {
	window, frame              uint32
	uploadBytes, largestUpload int
	processed                  bool
	err                        error
}

func reviewH2Peer(t *testing.T, responseBytes, responseFrame int) (string, func(), <-chan reviewH2Observation) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(func() { release(); _ = listener.Close() })
	finished := make(chan reviewH2Observation, 1)
	go func() {
		observed := reviewH2Observation{window: 65535, frame: 16384}
		defer func() { finished <- observed }()
		conn, err := listener.Accept()
		if err != nil {
			observed.err = err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		preface := make([]byte, len(peerh2.ClientPreface))
		if _, err := io.ReadFull(conn, preface); err != nil {
			observed.err = err
			return
		}
		if string(preface) != peerh2.ClientPreface {
			observed.err = errors.New("missing H2 preface")
			return
		}
		framer := peerh2.NewFramer(conn, conn)
		framer.SetMaxReadFrameSize(16384)
		if err := framer.WriteSettings(); err != nil {
			observed.err = err
			return
		}
		var stream uint32
		requestEnded := false
		for !requestEnded {
			frame, err := framer.ReadFrame()
			if err != nil {
				observed.err = err
				return
			}
			switch frame := frame.(type) {
			case *peerh2.SettingsFrame:
				if !frame.IsAck() {
					if value, ok := frame.Value(peerh2.SettingInitialWindowSize); ok {
						observed.window = value
					}
					if value, ok := frame.Value(peerh2.SettingMaxFrameSize); ok {
						observed.frame = value
					}
					if err := framer.WriteSettingsAck(); err != nil {
						observed.err = err
						return
					}
				}
			case *peerh2.HeadersFrame:
				stream = frame.StreamID
				requestEnded = frame.StreamEnded()
			case *peerh2.DataFrame:
				observed.uploadBytes += len(frame.Data())
				observed.largestUpload = max(observed.largestUpload, len(frame.Data()))
				requestEnded = frame.StreamEnded()
			}
		}
		var block bytes.Buffer
		encoder := hpack.NewEncoder(&block)
		_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
		_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: strconv.Itoa(responseBytes)})
		if err := framer.WriteHeaders(peerh2.HeadersFrameParam{StreamID: stream, EndHeaders: true, BlockFragment: block.Bytes()}); err != nil {
			observed.err = err
			return
		}
		<-gate
		if responseBytes == 0 {
			if err := framer.WriteData(stream, true, nil); err != nil {
				observed.err = err
				return
			}
		}
		for sent := 0; sent < responseBytes; {
			count := min(responseFrame, responseBytes-sent)
			if err := framer.WriteData(stream, sent+count == responseBytes, bytes.Repeat([]byte("r"), count)); err != nil {
				observed.err = err
				return
			}
			sent += count
		}
		probe := [8]byte{'a', 'f', 't', 'e', 'r', 'a', 'l', 'l'}
		if err := framer.WritePing(false, probe); err != nil {
			observed.err = err
			return
		}
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				observed.err = err
				return
			}
			switch frame := frame.(type) {
			case *peerh2.PingFrame:
				if frame.IsAck() && frame.Data == probe {
					observed.processed = true
					return
				}
			case *peerh2.GoAwayFrame:
				observed.err = fmt.Errorf("GOAWAY %s", frame.ErrCode)
				return
			}
		}
	}()
	return "http://" + listener.Addr().String(), release, finished
}

func reviewH2Transport(settings []nativeh2.Setting) *nativeh2.Transport {
	return &nativeh2.Transport{AllowHTTP: true, Settings: settings,
		DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}}
}

func TestH2OmittedAndZeroReceiveWindows(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		settings []nativeh2.Setting
		window   uint32
		size     int
		refused  bool
	}{
		{"generated-default", nil, 4 << 20, 1 << 20, false},
		{"omitted-exact-window", []nativeh2.Setting{{ID: nativeh2.SettingEnablePush, Val: 0}}, 65535, 65535, false},
		{"omitted-one-over", []nativeh2.Setting{{ID: nativeh2.SettingEnablePush, Val: 0}}, 65535, 65536, true},
		{"zero-empty-control", []nativeh2.Setting{{ID: nativeh2.SettingInitialWindowSize, Val: 0}}, 0, 0, false},
		{"zero-one-over", []nativeh2.Setting{{ID: nativeh2.SettingInitialWindowSize, Val: 0}}, 0, 1, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			endpoint, send, peer := reviewH2Peer(t, scenario.size, 16384)
			transport := reviewH2Transport(scenario.settings)
			defer transport.CloseIdleConnections()
			request, err := http.NewRequestWithContext(testContext(t), "GET", endpoint, nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := transport.RoundTrip(request)
			if err != nil {
				t.Fatal("response header control failed", err)
			}
			defer response.Body.Close()
			send()
			var observed reviewH2Observation
			select {
			case observed = <-peer:
			case <-testContext(t).Done():
				t.Fatal("peer did not finish")
			}
			data, readErr := io.ReadAll(response.Body)
			t.Logf("advertised=%d processed-before-read=%t received=%d read-error=%v", observed.window, observed.processed, len(data), readErr)
			if observed.window != scenario.window {
				t.Fatal("advertised receive window differs")
			}
			if scenario.refused {
				var protocolErr nativeh2.ConnectionError
				if observed.processed || !errors.As(readErr, &protocolErr) || protocolErr != nativeh2.ConnectionError(nativeh2.ErrCodeFlowControl) {
					t.Fatal("peer exceeded selected window without native flow-control refusal", observed.err, readErr)
				}
			} else if observed.err != nil || !observed.processed || readErr != nil || !bytes.Equal(data, bytes.Repeat([]byte("r"), scenario.size)) {
				t.Fatal("valid selected window rejected", observed.err, readErr)
			}
		})
	}
}

func TestH2FrameDirection(t *testing.T) {
	endpoint, send, peer := reviewH2Peer(t, 32768, 32768)
	transport := reviewH2Transport([]nativeh2.Setting{{ID: nativeh2.SettingMaxFrameSize, Val: 65536}, {ID: nativeh2.SettingInitialWindowSize, Val: 131072}})
	defer transport.CloseIdleConnections()
	request, err := http.NewRequestWithContext(testContext(t), "POST", endpoint, bytes.NewReader(bytes.Repeat([]byte("u"), 48<<10)))
	if err != nil {
		t.Fatal(err)
	}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal("client violated peer's default frame limit", err)
	}
	defer response.Body.Close()
	send()
	var observed reviewH2Observation
	select {
	case observed = <-peer:
	case <-testContext(t).Done():
		t.Fatal("peer did not finish")
	}
	data, readErr := io.ReadAll(response.Body)
	t.Logf("advertised-frame=%d largest-outbound=%d upload=%d response=%d processed=%t", observed.frame, observed.largestUpload, observed.uploadBytes, len(data), observed.processed)
	if observed.err != nil || readErr != nil || !observed.processed || observed.frame != 65536 || observed.largestUpload > 16384 || observed.uploadBytes != 48<<10 || len(data) != 32768 {
		t.Fatal("client confused local receive settings with peer outbound settings", observed.err, readErr)
	}
}

func TestH2InvalidSettingsCloseOwnedSocket(t *testing.T) {
	for _, setting := range []nativeh2.Setting{{ID: nativeh2.SettingInitialWindowSize, Val: 1 << 31}, {ID: nativeh2.SettingMaxFrameSize, Val: 16383}} {
		t.Run(setting.ID.String(), func(t *testing.T) {
			local, peer := net.Pipe()
			defer peer.Close()
			transport := reviewH2Transport([]nativeh2.Setting{setting})
			conn, err := transport.NewClientConn(local)
			if err == nil || conn != nil {
				t.Fatal("invalid local setting created a native connection")
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			var one [1]byte
			count, readErr := peer.Read(one[:])
			if count != 0 || !errors.Is(readErr, io.EOF) {
				t.Fatal("rejected settings left socket open or wrote bytes", count, readErr)
			}
		})
	}
}
