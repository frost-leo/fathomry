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

package tlsclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestProviderResponseIntegrityAndByteLimits(t *testing.T) {
	providerProtocols(t, func(t *testing.T, mode ProtocolMode) {
		for _, limited := range []bool{false, true} {
			t.Run(map[bool]string{false: "truncated", true: "limited"}[limited], func(t *testing.T) {
				endpoint, options := providerPeer(t, mode, func(w http.ResponseWriter, request *http.Request) {
					if !limited {
						w.Header().Set("Content-Length", "19")
					}
					_, _ = io.WriteString(w, "abcde")
				})
				if limited {
					options.MaxResponseBytes = 3
				}
				fixture := bindProvider(t, options, 1)
				receipt, err := fixture.client.Do(testContext(t), testContext(t), providerID("partial"), providerRequest(t, "GET", endpoint, nil))
				result := settleProvider(t, fixture, receipt)
				if err == nil || result.Err() == nil || result.Outcome.Value.Complete() {
					t.Fatal("incomplete body certified complete")
				}
				if limited && (!errors.Is(result.Err(), ErrLimit) || string(result.Outcome.Value.DataCopy()) != "abc") {
					t.Fatal("response limit lost", result.Err())
				}
				if !limited && (!errors.Is(result.Err(), ErrIntegrity) || !strings.HasPrefix(string(result.Outcome.Value.DataCopy()), "abc")) {
					t.Fatal("declared native length was trusted without EOF integrity", result.Err())
				}
			})
		}
	})
}
func TestProviderStreamBorrowingCancellationAndPartialEvidence(t *testing.T) {
	providerProtocols(t, func(t *testing.T, mode ProtocolMode) {
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		endpoint, options := providerPeer(t, mode, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "2")
			_, _ = io.WriteString(w, "a")
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		options.MaxActive = 1
		fixture := bindProvider(t, options, 2)
		ctx, cancel := context.WithCancel(testContext(t))
		defer cancel()
		stream, receipt, err := fixture.client.Open(ctx, providerID("stream"), providerRequest(t, "GET", endpoint, nil))
		if err != nil {
			t.Fatal(err)
		}
		short, stop := context.WithTimeout(context.Background(), time.Millisecond)
		defer stop()
		if _, err := receipt.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("headers released live stream", err)
		}
		if _, err := fixture.client.Do(testContext(t), testContext(t), providerID("refused"), providerRequest(t, "GET", endpoint, nil)); !errors.Is(err, resource.ErrCapacity) {
			t.Fatal("stream did not consume shared admission", err)
		}
		if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("live stream source closed", err)
		}
		data := make([]byte, 1)
		if count, err := stream.Read(data); count != 1 || err != nil || string(data) != "a" {
			t.Fatal("stream bytes lost", err)
		}
		cancel()
		if _, err := stream.Read(data); !errors.Is(err, context.Canceled) {
			t.Fatal("body cancellation lost", err)
		}
		if err := stream.Close(testContext(t)); !errors.Is(err, context.Canceled) {
			t.Fatal("cleanup erased primary cancellation", err)
		}
		result := settleProvider(t, fixture, receipt)
		if result.Outcome.Value.Complete() || result.Outcome.Value.DataCopy() != nil || result.Outcome.Value.BytesRead() != 1 {
			t.Fatal("partial stream evidence differs")
		}
		once.Do(func() { close(release) })
	})
}

type blockedCloseInput struct {
	entered, release chan struct{}
	once             sync.Once
}

func (input *blockedCloseInput) Read([]byte) (int, error) { return 0, io.EOF }
func (input *blockedCloseInput) Close() error {
	input.once.Do(func() { close(input.entered) })
	<-input.release
	return nil
}
func TestProviderBlockedInputCloseDoesNotReleaseEarly(t *testing.T) {
	endpoint, options := providerPeer(t, HTTP1Only, func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) })
	fixture := bindProvider(t, options, 1)
	input := &blockedCloseInput{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	defer once.Do(func() { close(input.release) })
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	returned := make(chan *invocation.Receipt[Result], 1)
	go func() {
		receipt, _ := fixture.client.Do(ctx, testContext(t), providerID("close"), providerRequest(t, "POST", endpoint, input))
		returned <- receipt
	}()
	select {
	case <-input.entered:
	case <-testContext(t).Done():
		t.Fatal("native body Close not reached")
	}
	cancel()
	var receipt *invocation.Receipt[Result]
	select {
	case receipt = <-returned:
	case <-testContext(t).Done():
		t.Fatal("caller wait did not stop")
	}
	if receipt == nil {
		t.Fatal("accepted call lost receipt")
	}
	if result, ok := receipt.Result(); ok && result.Released {
		t.Fatal("blocked native Close released receipt")
	}
	if err := fixture.assembly.Close(testContext(t)); !errors.Is(err, resource.ErrIncomplete) {
		t.Fatal("source released blocked native Close", err)
	}
	once.Do(func() { close(input.release) })
	settleProvider(t, fixture, receipt)
}
