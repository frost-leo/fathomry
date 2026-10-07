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
	fhttp "github.com/bogdanfinn/fhttp"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

type untouchedBody struct{ reads, closed atomic.Int64 }

func (body *untouchedBody) Read([]byte) (int, error) { body.reads.Add(1); return 0, io.EOF }
func (body *untouchedBody) Close() error             { body.closed.Add(1); return nil }

func TestPublicQueuedCancellationAndSaturationStayBeforeNativeWork(t *testing.T) {
	var requests atomic.Int64
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", "4")
		_, _ = io.WriteString(w, "body")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer peer.Close()
	prepared, err := Prepare(Settings{Name: "queued", MaxActive: pointer(1), QueuedCalls: pointer(1)}, testNative())
	if err != nil {
		t.Fatal(err)
	}
	deps := testMechanisms(t, prepared)
	policy, _ := prepared.Policy()
	policy.Evidence.Capacity++
	policy.Evidence.MaxBytes += policy.Budget.EvidenceBytes
	deps.Evidence, err = adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(testContext(t), deps)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close(context.Background())
	input, _ := fhttp.NewRequest("GET", peer.URL, nil)
	stream, first, err := owner.Client().Open(testContext(t), input)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close(context.Background())
	queued, cancel := context.WithCancel(testContext(t))
	defer cancel()
	type completion struct {
		receipt *adapters.Receipt[Result]
		err     error
	}
	done := make(chan completion, 1)
	go func() {
		receipt, err := owner.Client().Do(queued, context.Background(), input)
		done <- completion{receipt, err}
	}()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	until := testContext(t)
	for {
		status, err := deps.Runtime.Inspect()
		if err != nil {
			t.Fatal(err)
		}
		if status.Queued == 1 {
			break
		}
		select {
		case <-ticker.C:
		case <-until.Done():
			t.Fatal("public queue was not reached")
		}
	}
	body := new(untouchedBody)
	blocked, _ := fhttp.NewRequest("POST", peer.URL, body)
	if receipt, err := owner.Client().Do(testContext(t), testContext(t), blocked); receipt != nil || !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("saturated queue admitted another operation", err)
	}
	if requests.Load() != 1 || body.reads.Load() != 0 || body.closed.Load() != 0 {
		t.Fatal("pre-admission refusal touched native work or borrowed body")
	}
	cancel()
	select {
	case result := <-done:
		if result.receipt != nil || !errors.Is(result.err, context.Canceled) {
			t.Fatal("queued cancellation retained an operation", result.err)
		}
	case <-until.Done():
		t.Fatal("queued cancellation did not return")
	}
	if err := owner.Close(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if !owner.ShutdownComplete() {
		t.Fatal("owner cleanup did not continue with a live stream")
	}
	if _, err := first.WaitReleased(testContext(t)); err != nil {
		t.Fatal(err)
	}
	status, _ := deps.Runtime.Inspect()
	if status.Active != 0 || status.Queued != 0 || status.WorkBytes != 0 || requests.Load() != 1 {
		t.Fatal("canceled queue/source retained work or sent another request")
	}
	for {
		custody, _ := deps.Evidence.Inspect()
		if custody.Outstanding == 0 {
			break
		}
		delivery, err := deps.Evidence.NextReleased(testContext(t))
		if err != nil {
			t.Fatal(err)
		}
		if err := delivery.Ack(); err != nil {
			t.Fatal(err)
		}
	}
}
