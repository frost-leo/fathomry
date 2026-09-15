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

package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	http "github.com/sardanioss/http"
	"github.com/sardanioss/httpcloak/dns"
	"github.com/sardanioss/httpcloak/fingerprint"
	"io"
	"strings"
	"testing"
	"time"
)

func TestFathomryRaceJoinsLosingDial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	loserStarted := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		_, _ = staggeredRace(ctx, 2, time.Millisecond, func(ctx context.Context, index int) (int, error) {
			if index == 0 {
				<-loserStarted
				return 1, nil
			}
			close(loserStarted)
			<-release
			return 0, context.Canceled
		}, func(int) {})
	}()
	<-loserStarted
	select {
	case <-returned:
		t.Fatal("race returned before losing dial exited")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("race did not join released loser")
	}
}
func TestFathomryHeaderBoundDoesNotLimitBody(t *testing.T) {
	for _, valid := range []bool{false, true} {
		header := "HTTP/1.1 200 OK\r\nX: " + strings.Repeat("a", 100) + "\r\n\r\n"
		limit := int64(32)
		if valid {
			limit = int64(len(header))
		}
		reader := &headerLimitReader{reader: strings.NewReader(header + strings.Repeat("b", 200)), remaining: limit}
		data, err := io.ReadAll(reader)
		if valid && (err != nil || len(data) != len(header)+200) {
			t.Fatal("body was truncated by header budget")
		}
		if !valid && (err == nil || errors.Is(err, io.EOF) || len(data) > int(limit)) {
			t.Fatal("header overflow accepted")
		}
	}
}

func TestFathomryExactSpecialHeadersKeepCaseAndSingularity(t *testing.T) {
	request, _ := http.NewRequest("POST", "http://example.invalid", strings.NewReader("abc"))
	preset := fingerprint.GetStrict("chrome-148")
	applyExactHeaders(request, []fingerprint.HeaderPair{{Key: "content-length", Value: "3"}, {Key: "connection", Value: "close"}}, preset, nil, "h1")
	var output bytes.Buffer
	connection := &http1Conn{bw: bufio.NewWriter(&output)}
	transport := &HTTP1Transport{preset: preset}
	if err := transport.writeRequest(connection, request); err != nil {
		t.Fatal(err)
	}
	_ = connection.bw.Flush()
	headers := strings.SplitN(output.String(), "\r\n\r\n", 2)[0]
	if strings.Count(strings.ToLower(headers), "\r\ncontent-length:") != 1 || !strings.Contains(headers, "\r\ncontent-length: 3") || !strings.Contains(headers, "\r\nconnection: close") {
		t.Fatal("exact framing was duplicated or recased")
	}
}
func TestFathomryECHStateIsInstanceLocal(t *testing.T) {
	const host = "fathomry-ech-test.invalid"
	dns.MarkECHIncompatible(host)
	config := &TransportConfig{FathomryMaxHeaderBytes: 1024, ECHConfig: []byte{1, 2, 3}}
	var first, second echFailures
	if first.disabled(config, host) || second.disabled(config, host) {
		t.Fatal("global ECH state crossed managed boundary")
	}
	first.mark(config, host)
	if !first.disabled(config, host) || second.disabled(config, host) {
		t.Fatal("ECH state crossed owned instances")
	}
	transport := &HTTP3Transport{config: config, echConfigCache: make(map[string]*echCachedConfig)}
	if !bytes.Equal(transport.getECHConfig(context.Background(), host), config.ECHConfig) {
		t.Fatal("global ECH state suppressed explicit input")
	}
}

func TestFathomryKnownLengthBoundsSerializedBody(t *testing.T) {
	for _, payload := range []string{"abc", "ab", "abcGET /extra HTTP/1.1\r\nHost: example.invalid\r\n\r\n"} {
		request, _ := http.NewRequest("POST", "http://example.invalid", strings.NewReader(payload))
		request.ContentLength = 3
		var output bytes.Buffer
		connection := &http1Conn{bw: bufio.NewWriter(&output)}
		transport := &HTTP1Transport{preset: fingerprint.GetStrict("chrome-148")}
		err := transport.writeRequest(connection, request)
		_ = connection.bw.Flush()
		parts := bytes.SplitN(output.Bytes(), []byte("\r\n\r\n"), 2)
		if len(parts) != 2 {
			t.Fatal("request header boundary missing")
		}
		if len(parts[1]) > 3 || (err == nil) != (payload == "abc") {
			t.Errorf("known-length writer failed to bound/validate serialized body: input=%d wire=%d error=%v", len(payload), len(parts[1]), err)
		}
	}
}

func TestFathomryConnectionCloseFieldsAreAuthoritative(t *testing.T) {
	transport := &HTTP1Transport{preset: fingerprint.GetStrict("chrome-148")}
	request, _ := http.NewRequest("GET", "http://example.invalid", nil)
	response := &http.Response{ProtoMajor: 1, ProtoMinor: 1, Header: make(http.Header), Close: true}
	if transport.shouldKeepAlive(request, response) {
		t.Error("parsed response Close was ignored")
	}
	response.Close = false
	request.Close = true
	if transport.shouldKeepAlive(request, response) {
		t.Error("request Close was ignored")
	}
	var output bytes.Buffer
	if err := transport.writeRequest(&http1Conn{bw: bufio.NewWriter(&output)}, request); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "Connection: close\r\n") {
		t.Error("request Close field did not reach the wire")
	}
}

func TestFathomryOrderedCredentialRemovalKeepsMapInactive(t *testing.T) {
	preset := fingerprint.GetStrict("chrome-148")
	preset.Headers = map[string]string{"X-Inactive": "not-selected"}
	preset.HeaderOrder = []fingerprint.HeaderPair{{Key: "Authorization", Value: "test"}}
	managed, err := NewFathomryTransport(preset, &TransportConfig{}, ProtocolHTTP1, nil, false, false, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer managed.Close()
	request, _ := http.NewRequest("GET", "http://example.invalid", nil)
	applyPresetHeaders(request, managed.preset, nil, nil, false, "h1", nil, false, nil)
	if request.Header.Get("X-Inactive") != "" || len(preset.HeaderOrder) != 1 || preset.Headers["X-Inactive"] == "" {
		t.Fatal("credential removal changed the selected header source or mutated its input")
	}
}

type terminalBody struct {
	readErr, closeErr error
	read              bool
}

func (body *terminalBody) Read(data []byte) (int, error) {
	if body.read {
		return 0, io.EOF
	}
	body.read = true
	return copy(data, "abc"), body.readErr
}
func (body *terminalBody) Close() error { return body.closeErr }

func TestFathomryKnownLengthRetainsTerminalAndCloseFailures(t *testing.T) {
	readErr, closeErr := errors.New("test terminal read"), errors.New("test close")
	request, _ := http.NewRequest("POST", "http://example.invalid", &terminalBody{readErr: readErr, closeErr: closeErr})
	request.ContentLength = 3
	var output bytes.Buffer
	transport := &HTTP1Transport{preset: fingerprint.GetStrict("chrome-148")}
	err := transport.writeRequest(&http1Conn{bw: bufio.NewWriter(&output)}, request)
	if !errors.Is(err, readErr) || !errors.Is(err, closeErr) {
		t.Fatal("declared-length boundary lost terminal read or cleanup evidence")
	}
}

func TestFathomryCancellationRetainsBodyErrorAndCause(t *testing.T) {
	cause, bodyErr := errors.New("test cancellation cause"), errors.New("test native body")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	err := translateBodyError(ctx, bodyErr)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || !errors.Is(err, bodyErr) {
		t.Fatal("body cancellation replaced original failure or cause")
	}
}
