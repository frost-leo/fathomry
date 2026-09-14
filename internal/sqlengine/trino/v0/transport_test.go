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

package trino

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/trinodb/trino-go-client/trino"
)

func TestNoHTTPReplayAndLostSubmission(t *testing.T) {
	for _, status := range []int{429, 502, 503, 504, 307, 401} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var posts atomic.Int64
			_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				w.Header().Set("Location", "http://should-not-contact.invalid/")
				w.WriteHeader(status)
			})
			f := bindFixture(t, o, 1)
			receipt, err := f.client.Execute(deadline(t), deadline(t), correlation("write"), Statement{SQL: "INSERT INTO data VALUES (1)"})
			result := settle(t, receipt, err)
			var nativeError *native.ErrQueryFailed
			if !errors.As(result.Err(), &nativeError) || nativeError.StatusCode != status ||
				result.Outcome.Value.Effect() != Unknown || result.Outcome.Value.Terminal() ||
				result.Outcome.Value.Submissions() != 1 || posts.Load() != 1 {
				t.Fatal("HTTP failure replayed or promoted to effect certainty")
			}
		})
	}
	t.Run("lost-reply", func(t *testing.T) {
		var posts atomic.Int64
		_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
			posts.Add(1)
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
		})
		f := bindFixture(t, o, 1)
		receipt, err := f.client.Execute(deadline(t), deadline(t), correlation("lost"), Statement{SQL: "UPDATE data SET x=1"})
		result := settle(t, receipt, err)
		if result.Err() == nil || result.Outcome.Value.Effect() != Unknown || posts.Load() != 1 ||
			result.Outcome.Value.CancellationAttempted() {
			t.Fatal("lost acknowledgement was retried or invented an ID")
		}
	})
}
func TestLateTerminalErrorAndAggregateCount(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			var base string
			var gets atomic.Int64
			_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					reply(t, w, map[string]any{"id": "late", "nextUri": base + "/v1/statement/executing/late/slug/1", "updateCount": 19})
				case http.MethodGet:
					if gets.Add(1) == 1 {
						reply(t, w, map[string]any{"id": "late", "nextUri": base + "/v1/statement/executing/late/slug/2", "updateCount": 7})
						return
					}
					page := map[string]any{"id": "late", "updateCount": int64(0)}
					if bad {
						page["error"] = map[string]any{"errorCode": 123, "errorName": "SYNTHETIC_ERROR", "errorType": "EXTERNAL", "message": "private-query-canary"}
					}
					reply(t, w, page)
				default:
					t.Error("terminal request unnecessarily cancelled")
				}
			})
			base = o.Endpoint
			f := bindFixture(t, o, 1)
			receipt, err := f.client.Execute(deadline(t), deadline(t), correlation("late"), Statement{SQL: "MERGE INTO data USING src ON data.id=src.id WHEN MATCHED THEN DELETE"})
			result := settle(t, receipt, err)
			value := result.Outcome.Value
			if gets.Load() != 2 || value.Pages() != 3 || !value.Terminal() {
				t.Fatal("late pages not drained")
			}
			if bad {
				var cause *native.ErrTrino
				if !errors.As(result.Err(), &cause) || cause.ErrorCode != 123 || value.Effect() != Unknown ||
					value.Succeeded() || value.Complete() || strings.Contains(fmt.Sprintf("%+v", result.Err()), "private-query-canary") {
					t.Fatal("late native failure/effect/privacy lost")
				}
				if _, known := value.UpdateCount(); known {
					t.Fatal("failed terminal retained an intermediate count")
				}
			} else {
				success(t, result)
				if count, known := value.UpdateCount(); count != 0 || !known {
					t.Fatal("terminal zero conflated with intermediate/absent count")
				}
			}
		})
	}
}
func TestCancellationJoinsNativeWorkBeforeBoundedRemoteCleanup(t *testing.T) {
	for _, cancelStatus := range []int{204, 503} {
		t.Run(fmt.Sprint(cancelStatus), func(t *testing.T) {
			started := make(chan struct{})
			getEnded := make(chan struct{})
			var base string
			var deletes atomic.Int64
			_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPost:
					reply(t, w, map[string]any{"id": "cancel", "nextUri": base + "/v1/statement/executing/cancel/slug/1"})
				case http.MethodGet:
					close(started)
					<-r.Context().Done()
					close(getEnded)
				case http.MethodDelete:
					deletes.Add(1)
					w.WriteHeader(cancelStatus)
				}
			})
			base = o.Endpoint
			f := bindFixture(t, o, 1)
			ctx, cancel := context.WithCancel(deadline(t))
			defer cancel()
			cleanup := deadline(t)
			done := make(chan invocation.Result[Result], 1)
			go func() {
				receipt, err := f.client.Execute(ctx, cleanup, correlation("cancel"), Statement{SQL: "DELETE FROM data"})
				if err != nil {
					done <- invocation.Result[Result]{Outcome: invocation.Outcome[Result]{Primary: err}}
					return
				}
				result, _ := receipt.Result()
				done <- result
			}()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("GET did not start")
			}
			cancel()
			var result invocation.Result[Result]
			select {
			case result = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("native cancellation failed to terminate")
			}
			select {
			case <-getEnded:
			case <-time.After(time.Second):
				t.Fatal("GET was not cancelled")
			}
			value := result.Outcome.Value
			if !errors.Is(result.Err(), context.Canceled) || !result.Released || !result.Final ||
				value.Terminal() || value.Succeeded() || value.Effect() != Unknown ||
				!value.CancellationAttempted() || value.CancellationAcknowledged() != (cancelStatus == 204) ||
				deletes.Load() != 1 || result.Attempts.Exact || result.Attempts.Observed != 3 {
				t.Fatalf("cancellation facts changed: released=%t terminal=%t ack=%t delete=%d attempts=%d primary-cancel=%t",
					result.Released, value.Terminal(), value.CancellationAcknowledged(), deletes.Load(), result.Attempts.Observed, errors.Is(result.Err(), context.Canceled))
			}
			if (result.Outcome.Cleanup != nil) != (cancelStatus != 204) {
				t.Fatal("cleanup failure lost")
			}
		})
	}
}
func TestRoutingSpoolingSessionAndMalformedPages(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		header   string
		identity error
	}{
		{"cross-origin", `{"id":"guard","nextUri":"http://escape.invalid/v1/statement/executing/guard/slug/1"}`, "", ErrAuthority},
		{"path-escape", `{"id":"guard","nextUri":"BASE/v1/statement/../query/guard"}`, "", ErrAuthority},
		{"spooling", `{"id":"guard","data":{"encoding":"json","segments":[{"type":"spooled","uri":"http://escape.invalid/segment","ackUri":"http://escape.invalid/ack"}]}}`, "", ErrUnsupported},
		{"session", `{"id":"guard"}`, "X-Trino-Set-Session", ErrUnsupported},
		{"transaction", `{"id":"guard"}`, "X-Trino-Started-Transaction-Id", ErrUnsupported},
		{"duplicate", `{"id":"guard","id":"other"}`, "", ErrProtocol},
		{"trailing", `{"id":"guard"} {}`, "", ErrProtocol},
		{"error-empty", `{"id":"guard","error":{}}`, "", ErrProtocol},
		{"negative-count", `{"id":"guard","updateCount":-1}`, "", ErrProtocol},
		{"ragged", `{"id":"guard","columns":[{"name":"x","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}],"data":[[]]}`, "", ErrProtocol},
		{"unsupported", `{"id":"guard","columns":[{"name":"x","type":"variant","typeSignature":{"rawType":"variant","arguments":[]}}],"data":[[null]]}`, "", ErrUnsupported},
		{"integer-overflow", `{"id":"guard","columns":[{"name":"x","type":"bigint","typeSignature":{"rawType":"bigint","arguments":[]}}],"data":[[9223372036854775808]]}`, "", ErrProtocol},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int64
			var base string
			_, o := peer(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if r.Method == http.MethodDelete {
					w.WriteHeader(204)
					return
				}
				if r.Method != http.MethodPost {
					t.Error("unexpected download or GET")
				}
				w.Header().Set("Content-Type", "application/json")
				if test.header != "" {
					w.Header().Set(test.header, "synthetic-private")
				}
				_, _ = io.WriteString(w, strings.ReplaceAll(test.payload, "BASE", base))
			})
			base = o.Endpoint
			f := bindFixture(t, o, 1)
			receipt, err := f.client.Query(deadline(t), deadline(t), correlation("guard"), Statement{SQL: "SELECT * FROM data"})
			result := settle(t, receipt, err)
			if !errors.Is(result.Err(), test.identity) || result.Outcome.Value.Complete() || requests.Load() > 2 {
				t.Fatal("guard failed or issued an unbounded request")
			}
		})
	}
}
