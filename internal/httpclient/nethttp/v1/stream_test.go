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

package nethttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestStreamRetainsResourceAfterHeadersAndWaitCancellation(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		release := make(chan struct{})
		defer close(release)
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Length", "2")
			_, _ = io.WriteString(writer, "a")
			writer.(http.Flusher).Flush()
			<-release
		})
		f := bindFixture(t, options, 1)
		stream, receipt, err := f.client.Open(deadline(t), correlation("stream"), newRequest(t, "GET", server.URL, nil))
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close(deadline(t))
		wait, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		if _, err := receipt.Wait(wait); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("headers falsely completed the response")
		}
		if err := f.assembly.Close(deadline(t)); !errors.Is(err, resource.ErrIncomplete) {
			t.Fatal("live stream did not retain its source", err)
		}
		body := make([]byte, 1)
		if count, err := stream.Read(body); count != 1 || err != nil || string(body) != "a" {
			t.Fatal("stream body lost", err)
		}
		if err := stream.Close(deadline(t)); err != nil {
			t.Fatal(err)
		}
		result := settle(t, f, receipt)
		if result.Outcome.Value.Complete() || result.Outcome.Value.DataCopy() != nil || result.Outcome.Value.BytesRead() != 1 {
			t.Fatal("abandoned stream became complete retained data")
		}
	})
}

func TestBodyLimitAndTruncationRemainPartial(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		for _, limited := range []bool{false, true} {
			t.Run(map[bool]string{false: "truncated", true: "limited"}[limited], func(t *testing.T) {
				server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
					if !limited {
						writer.Header().Set("Content-Length", "9")
					}
					_, _ = io.WriteString(writer, "abcde")
				})
				if limited {
					options.MaxResponseBytes = 3
				}
				f := bindFixture(t, options, 1)
				receipt, err := f.client.Do(deadline(t), deadline(t), correlation("partial"), newRequest(t, "GET", server.URL, nil))
				result := settle(t, f, receipt)
				if err == nil || result.Err() == nil || result.Outcome.Value.Complete() {
					t.Fatal("incomplete body accepted")
				}
				if limited && (!errors.Is(result.Err(), ErrLimit) || string(result.Outcome.Value.DataCopy()) != "abc") {
					t.Fatal("body limit lost", result.Err())
				}
				if !limited && !strings.HasPrefix(string(result.Outcome.Value.DataCopy()), "abc") {
					t.Fatal("partial bytes lost")
				}
			})
		}
	})
}

func TestBodyDeadlineRetainsPartialData(t *testing.T) {
	eachProtocol(t, func(t *testing.T, h2 bool) {
		release := make(chan struct{})
		defer close(release)
		server, options := newPeer(t, h2, func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Length", "2")
			_, _ = io.WriteString(writer, "a")
			writer.(http.Flusher).Flush()
			<-release
		})
		options.Timeout = 100 * time.Millisecond
		f := bindFixture(t, options, 1)
		receipt, err := f.client.Do(deadline(t), deadline(t), correlation("deadline"), newRequest(t, "GET", server.URL, nil))
		if err == nil {
			t.Fatal("body deadline was not enforced")
		}
		result := settle(t, f, receipt)
		if result.Outcome.Value.Complete() || string(result.Outcome.Value.DataCopy()) != "a" || !errors.Is(result.Err(), context.DeadlineExceeded) {
			t.Fatal("deadline evidence lost", result.Err())
		}
		if f.inbox.Usage() != (invocation.InboxUsage{}) {
			t.Fatal("evidence capacity retained")
		}
	})
}
