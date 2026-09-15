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

package tls_client

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	http "github.com/bogdanfinn/fhttp"
)

type compatTestTransport func(*http.Request) (*http.Response, error)

func (send compatTestTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return send(request)
}

type compatTestBody struct {
	io.Reader
	closes atomic.Int64
}

func (body *compatTestBody) Close() error { body.closes.Add(1); return nil }
func compatTestRacer(t *testing.T, ctx context.Context, h3, h2 http.RoundTripper) (*protocolRacer, *roundTripper, context.Context, func()) {
	t.Helper()
	state := newCompatibilityState()
	work, done, err := state.begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rt := &roundTripper{compat: state, cachedTransports: map[string]http.RoundTripper{"peer.invalid:443": h2, "peer.invalid:443:h3": h3}, cachedConnections: make(map[string]net.Conn)}
	racer := &protocolRacer{compat: state, protocolCache: make(map[string]string), cachedTransports: rt.cachedTransports, cachedTransportsLck: &rt.cachedTransportsLck}
	return racer, rt, work, done
}

func TestFathomryRacingClonesHeadersAndReplayBodies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requests := make(chan *http.Request, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	send := compatTestTransport(func(request *http.Request) (*http.Response, error) {
		requests <- request
		select {
		case <-release:
		case <-request.Context().Done():
			return nil, request.Context().Err()
		}
		data, err := io.ReadAll(request.Body)
		if err != nil {
			return nil, err
		}
		if string(data) != "payload" {
			return nil, errors.New("independent replay body changed")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("complete"))}, nil
	})
	racer, rt, work, done := compatTestRacer(t, ctx, send, send)
	defer func() {
		done()
		if err := rt.compat.close(rt); err != nil {
			t.Error(err)
		}
	}()
	request, _ := http.NewRequestWithContext(work, "POST", "https://peer.invalid/", strings.NewReader("payload"))
	request.Header = http.Header{"X-Input": {"original"}}
	request.Trailer = http.Header{"X-Trailer": {"original"}}
	result := make(chan *http.Response, 1)
	failure := make(chan error, 1)
	go func() {
		response, err := racer.race(request, "peer.invalid:443", nil)
		result <- response
		failure <- err
	}()
	first, second := <-requests, <-requests
	first.Header.Set("X-Input", "first")
	first.Trailer.Set("X-Trailer", "first")
	if second.Header.Get("X-Input") != "original" || second.Trailer.Get("X-Trailer") != "original" || request.Header.Get("X-Input") != "original" {
		unblock()
		response := <-result
		if response != nil {
			_ = response.Body.Close()
		}
		<-failure
		t.Fatal("racing request metadata aliases another leg or caller")
	}
	if first.Body == second.Body {
		t.Fatal("racing legs share a body reader")
	}
	unblock()
	response := <-result
	if err := <-failure; err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFathomryWinnerContextSurvivesHeaders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	winner := make(chan context.Context, 1)
	send := compatTestTransport(func(request *http.Request) (*http.Response, error) {
		winner <- request.Context()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("stream"))}, nil
	})
	loser := compatTestTransport(func(request *http.Request) (*http.Response, error) { return nil, errors.New("unexpected delayed leg") })
	racer, rt, work, done := compatTestRacer(t, ctx, send, loser)
	defer func() {
		done()
		if err := rt.compat.close(rt); err != nil {
			t.Error(err)
		}
	}()
	request, _ := http.NewRequestWithContext(work, "GET", "https://peer.invalid/", nil)
	response, err := racer.race(request, "peer.invalid:443", nil)
	if err != nil {
		t.Fatal(err)
	}
	winnerContext := <-winner
	if winnerContext.Err() != nil {
		t.Fatal("winner was canceled at header handoff")
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if winnerContext.Err() == nil {
		t.Fatal("closing winner did not end its lifetime")
	}
}

func TestFathomryCancellationOwnsLateLosingResponse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	body := &compatTestBody{Reader: strings.NewReader("late")}
	h3 := compatTestTransport(func(request *http.Request) (*http.Response, error) {
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, Body: body}, nil
	})
	var delayed atomic.Int64
	h2 := compatTestTransport(func(request *http.Request) (*http.Response, error) {
		delayed.Add(1)
		return nil, errors.New("delayed leg ran")
	})
	racer, rt, work, done := compatTestRacer(t, ctx, h3, h2)
	request, _ := http.NewRequestWithContext(work, "GET", "https://peer.invalid/", nil)
	response, err := racer.race(request, "peer.invalid:443", nil)
	if response != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("caller deadline was not returned", err)
	}
	<-entered
	done()
	closed := make(chan error, 1)
	go func() { closed <- rt.compat.close(rt) }()
	select {
	case <-closed:
		t.Fatal("shutdown abandoned a live racing leg")
	case <-time.After(20 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late cleanup did not join")
	}
	if body.closes.Load() != 1 || delayed.Load() != 0 {
		t.Fatal("late body lost or canceled delay submitted work")
	}
}

func TestFathomryRejectsUnreplayableRacingBody(t *testing.T) {
	for _, invalid := range []string{"missing", "nil", "typed-nil"} {
		t.Run(invalid, func(t *testing.T) {
			var sends atomic.Int64
			send := compatTestTransport(func(*http.Request) (*http.Response, error) { sends.Add(1); return nil, nil })
			racer, rt, work, done := compatTestRacer(t, context.Background(), send, send)
			defer func() { done(); _ = rt.compat.close(rt) }()
			body := &compatTestBody{Reader: strings.NewReader("payload")}
			request, _ := http.NewRequestWithContext(work, "POST", "https://peer.invalid/", body)
			if invalid == "nil" {
				request.GetBody = func() (io.ReadCloser, error) { return nil, nil }
			}
			if invalid == "typed-nil" {
				request.GetBody = func() (io.ReadCloser, error) { var value *compatTestBody; return value, nil }
			}
			response, err := racer.race(request, "peer.invalid:443", nil)
			if response != nil || !errors.Is(err, ErrBodyNotReplayable) || sends.Load() != 0 || body.closes.Load() != 1 {
				t.Fatal("unsafe replay admitted", err)
			}
		})
	}
}
