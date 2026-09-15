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

package httpcloak

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/sardanioss/httpcloak/fingerprint"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

func TestH1OverlongInputCannotCrossDeclaredWireLength(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	wire := make(chan []byte, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			wire <- nil
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				wire <- nil
				return
			}
			if line == "\r\n" {
				break
			}
		}
		_, _ = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
		data, _ := io.ReadAll(reader)
		wire <- data
	}()
	fixture := bindFixture(t, OptionsV1{Name: "source", PresetName: "chrome-148", Protocol: HTTP1}, 1)
	payload := "abcGET /extra HTTP/1.1\r\nHost: example.invalid\r\n\r\n"
	input := request(t, "POST", "http://"+listener.Addr().String(), strings.NewReader(payload))
	input.ContentLength = 3
	receipt, _ := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "overlong"}, input)
	result := settle(t, fixture, receipt)
	select {
	case data := <-wire:
		if len(data) > 3 {
			t.Errorf("declared 3-byte request sent %d raw body bytes to peer", len(data))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("raw H1 peer did not exit")
	}
	if result.Outcome.Value.Complete() || !errors.Is(result.Err(), ErrIntegrity) {
		t.Fatal("overlong input did not retain integrity failure")
	}
}

func TestCredentialOnlyOrderedPresetDoesNotReactivateLegacyHeaders(t *testing.T) {
	for _, protocol := range []ProtocolMode{HTTP1, HTTP2, HTTP3} {
		address, options := peer(t, protocol, func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("X-Inactive") != "" {
				t.Error("inactive legacy preset headers were reactivated")
			}
			if r.Header.Get("Authorization") != "Bearer test" {
				t.Error("active ordered credential was lost")
			}
			_, _ = io.WriteString(w, "ok")
		})
		preset := fingerprint.GetStrict("chrome-148")
		preset.Headers = map[string]string{"X-Inactive": "not-selected"}
		preset.HeaderOrder = []fingerprint.HeaderPair{{Key: "Authorization", Value: "Bearer test"}}
		options.PresetName = ""
		options.Native.Preset = preset
		fixture := bindFixture(t, options, 1)
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "ordered-only"}, request(t, "GET", address, nil))
		if err != nil {
			t.Fatal(err)
		}
		settle(t, fixture, receipt)
	}
}

func TestH2ExactCookiePairsPreserveHPACKFields(t *testing.T) {
	_, options := peer(t, HTTP2, func(http.ResponseWriter, *http.Request) {})
	certPeer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer certPeer.Close()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: certPeer.TLS.Certificates, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	captured := make(chan []hpack.HeaderField, 3)
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Error("raw H2 peer did not exit")
		}
	})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(conn, preface); err != nil || string(preface) != http2.ClientPreface {
			return
		}
		framer := http2.NewFramer(conn, conn)
		framer.ReadMetaHeaders = hpack.NewDecoder(4096, nil)
		if err := framer.WriteSettings(); err != nil {
			return
		}
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				return
			}
			switch frame := frame.(type) {
			case *http2.SettingsFrame:
				if !frame.IsAck() {
					if err := framer.WriteSettingsAck(); err != nil {
						return
					}
				}
			case *http2.MetaHeadersFrame:
				captured <- append([]hpack.HeaderField(nil), frame.Fields...)
				var encoded bytes.Buffer
				encoder := hpack.NewEncoder(&encoded)
				_ = encoder.WriteField(hpack.HeaderField{Name: ":status", Value: "200"})
				_ = encoder.WriteField(hpack.HeaderField{Name: "content-length", Value: "2"})
				if err := framer.WriteHeaders(http2.HeadersFrameParam{StreamID: frame.StreamID, BlockFragment: encoded.Bytes(), EndHeaders: true}); err != nil {
					return
				}
				if err := framer.WriteData(frame.StreamID, true, []byte("ok")); err != nil {
					return
				}
			}
		}
	}()
	fixture := bindFixture(t, options, 1)
	address := "https://" + listener.Addr().String()
	for _, exact := range []bool{true, false, true} {
		input := request(t, "GET", address, nil)
		input.Header.Set("Cookie", "a=1; b=2")
		var requestOptions RequestOptionsV1
		expected := []string{"cookie:a=1", "cookie:b=2"}
		if exact {
			requestOptions.ExactHeaders = []fingerprint.HeaderPair{{Key: "Cookie", Value: "a=1; b=2"}, {Key: "X-Between", Value: "middle"}, {Key: "Cookie", Value: "c=3; d=4"}}
			expected = []string{"cookie:a=1; b=2", "x-between:middle", "cookie:c=3; d=4"}
		}
		receipt, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "hpack"}, input, requestOptions)
		if err != nil {
			t.Fatal(err)
		}
		settle(t, fixture, receipt)
		select {
		case fields := <-captured:
			var actual []string
			for _, field := range fields {
				if field.Name == "cookie" || field.Name == "x-between" {
					actual = append(actual, field.Name+":"+field.Value)
				}
			}
			if !reflect.DeepEqual(actual, expected) {
				t.Errorf("HPACK fields changed: got %q want %q", actual, expected)
			}
		case <-time.After(time.Second):
			t.Fatal("HPACK request fields not observed")
		}
	}
}

func TestH1ReplayFactoryRetainsResponseFailure(t *testing.T) {
	address, options := peer(t, HTTP1, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			_, _ = io.Copy(io.Discard, r.Body)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	fixture := bindFixture(t, options, 1)
	warm, err := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "warm"}, request(t, "GET", address, nil))
	if err != nil {
		t.Fatal(err)
	}
	settle(t, fixture, warm)
	cause := errors.New("test replay factory")
	input := request(t, "POST", address+"/fail", strings.NewReader("abc"))
	input.GetBody = func() (io.ReadCloser, error) { return nil, cause }
	receipt, _ := fixture.client.Do(testContext(t), testContext(t), fault.Correlation{Call: "replay-failure"}, input)
	result := settle(t, fixture, receipt)
	if result.Outcome.Value.Replays() != 1 || !errors.Is(result.Err(), cause) || !errors.Is(result.Err(), io.ErrUnexpectedEOF) {
		t.Fatal("failed replay discarded the initial response failure")
	}
}
