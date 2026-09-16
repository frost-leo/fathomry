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

package nuki

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	nativehttp "github.com/nukilabs/http"
)

func TestProviderRedirectReplayAndNoFollow(t *testing.T) {
	var received atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		data, err := io.ReadAll(request.Body)
		if err != nil || string(data) != "payload" || request.Header.Get("X-Input") != "preserved" {
			t.Error("replayed input changed", err)
		}
		received.Add(1)
		if request.URL.Path == "/first" {
			http.Redirect(writer, request, "/last", 307)
			return
		}
		_, _ = writer.Write(data)
	}))
	defer peer.Close()
	for _, follow := range []bool{false, true} {
		received.Store(0)
		options := providerOptions()
		options.FollowRedirects = follow
		fixture := bindProvider(t, options)
		request := nativeRequest(t, "POST", peer.URL+"/first", strings.NewReader("payload"))
		request.Header.Set("X-Input", "preserved")
		receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "redirect"}, request)
		if err != nil {
			t.Fatal(err)
		}
		result := outcome(t, fixture, receipt)
		want := int32(1)
		status := 307
		if follow {
			want, status = 2, 200
		}
		if result.Err() != nil || !result.Outcome.Value.Complete() || result.Outcome.Value.Metadata().StatusCode() != status || received.Load() != want ||
			result.Attempts.Observed != uint64(want) || result.Attempts.Exact {
			t.Fatal("redirect/attempt contract", result.Err(), received.Load())
		}
		if follow && (result.Outcome.Value.Replays() != 1 || result.Outcome.Value.BytesReadFromInput() != 14 || string(result.Outcome.Value.DataCopy()) != "payload") {
			t.Fatal("replay evidence mismatch")
		}
	}
}

func TestProviderReplayLimitsAliasingAndFactoryCause(t *testing.T) {
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		http.Redirect(writer, request, "/again", 307)
	}))
	defer peer.Close()
	cause := errors.New("synthetic-replay-factory")
	for _, kind := range []string{"limit", "alias", "cause", "nil"} {
		t.Run(kind, func(t *testing.T) {
			options := providerOptions()
			options.FollowRedirects = true
			options.MaxReplays = 1
			fixture := bindProvider(t, options)
			request := nativeRequest(t, "POST", peer.URL, strings.NewReader("data"))
			wanted := error(ErrLimit)
			switch kind {
			case "alias":
				original := request.Body
				request.GetBody = func() (io.ReadCloser, error) { return original, nil }
				wanted = ErrInput
			case "cause":
				request.GetBody = func() (io.ReadCloser, error) { return nil, cause }
				wanted = cause
			case "nil":
				request.GetBody = func() (io.ReadCloser, error) { return nil, nil }
				wanted = ErrInput
			}
			receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: "replay"}, request)
			if err != nil {
				t.Fatal(err)
			}
			result := outcome(t, fixture, receipt)
			if !errors.Is(result.Err(), wanted) || result.Outcome.Value.Complete() {
				t.Fatal("unsafe replay accepted or cause lost", result.Err())
			}
		})
	}
}

func TestProviderConcurrentInputsHooksAndMetadataIsolation(t *testing.T) {
	var before, after, accepted atomic.Int64
	peer := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		cookie, cookieErr := request.Cookie("session")
		if err != nil || cookieErr != nil || string(body) != request.Header.Get("X-Call") || cookie.Value != string(body) || request.Header.Get("X-Hook") != "yes" {
			t.Error("cross-request input", err, cookieErr)
		}
		accepted.Add(1)
		_, _ = writer.Write(body)
	}))
	defer peer.Close()
	options := providerOptions()
	options.MaxActive = 8
	options.QueuedCalls = 8
	options.Native.TLS = testRoots(peer)
	options.Native.Before = []func(context.Context, *nativehttp.Request) error{func(_ context.Context, request *nativehttp.Request) error {
		before.Add(1)
		request.Header.Set("X-Hook", "yes")
		return nil
	}}
	options.Native.After = []func(context.Context, Metadata) error{func(_ context.Context, metadata Metadata) error {
		after.Add(1)
		if metadata.StatusCode() != 200 {
			t.Error("post hook missing metadata")
		}
		return nil
	}}
	fixture := bindProvider(t, options)
	for batch := 0; batch < 8; batch++ {
		var group sync.WaitGroup
		receipts := make([]*invocation.Receipt[Result], 8)
		for index := range receipts {
			group.Add(1)
			go func(index int) {
				defer group.Done()
				value := fmt.Sprintf("call-%d-%d", batch, index)
				request := nativeRequest(t, "POST", peer.URL, strings.NewReader(value))
				request.Header.Set("X-Call", value)
				request.AddCookie(&nativehttp.Cookie{Name: "session", Value: value})
				receipt, err := fixture.client.Do(testContext(t), fault.Correlation{Call: value}, request)
				if err != nil {
					t.Error(err)
					return
				}
				request.Header.Set("X-Call", "caller-mutation")
				result, err := receipt.WaitReleased(testContext(t))
				if err != nil || result.Err() != nil || string(result.Outcome.Value.DataCopy()) != value {
					t.Error("isolated response mismatch", err, result.Err())
				}
				receipts[index] = receipt
			}(index)
		}
		group.Wait()
		for range receipts {
			delivery, err := fixture.inbox.Next(testContext(t))
			if err != nil {
				t.Fatal(err)
			}
			result, err := delivery.Receipt().WaitReleased(testContext(t))
			if err != nil || result.Err() != nil {
				t.Fatal("independent evidence lost", err, result.Err())
			}
			if err := delivery.Release(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if before.Load() != 64 || after.Load() != 64 || accepted.Load() != 64 {
		t.Fatal("native callbacks skipped", before.Load(), after.Load(), accepted.Load())
	}
}
