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

package otel

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
)

type endEntryContext struct {
	context.Context
	once    sync.Once
	entered chan struct{}
}

func (ctx *endEntryContext) Err() error {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Err()
}

func TestConcurrentSpanEndHonorsItsOwnCanceledWait(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-request.Context().Done():
			return
		}
		writer.Header().Set("Content-Type", "application/x-protobuf")
	}))
	t.Cleanup(server.Close)
	t.Cleanup(unblock)
	config := Settings{Name: "end-wait", ServiceName: "fixture", LogsEndpoint: server.URL + "/logs", TracesEndpoint: server.URL + "/traces"}
	policy, err := Recommend(config)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := Open(context.Background(), config, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	var span *Span
	t.Cleanup(func() {
		unblock()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if span != nil {
			_, _ = span.End(ctx)
		}
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := inbox.Seal(); err != nil {
			t.Error(err)
		}
		for {
			status, err := inbox.Inspect()
			if err != nil {
				t.Error(err)
				break
			}
			if status.Outstanding == 0 {
				break
			}
			delivery, err := inbox.NextReleased(ctx)
			if err != nil {
				t.Error(err)
				break
			}
			if err := delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	_, span, err = owner.Client().Start(context.Background(), SpanInput{Name: "retained"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Client().Emit(context.Background(), LogRecord{Message: "gate"}); err != nil {
		t.Fatal(err)
	}
	flushDone := make(chan struct{})
	go func() { _, _ = owner.Client().Flush(context.Background()); close(flushDone) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("source gate was not entered")
	}
	firstDone := make(chan error, 1)
	firstContext := &endEntryContext{Context: context.Background(), entered: make(chan struct{})}
	go func() { _, err := span.End(firstContext); firstDone <- err }()
	select {
	case <-firstContext.entered:
	case <-time.After(time.Second):
		t.Fatal("first End did not reach native cleanup context admission")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() { _, err := span.End(ctx); secondDone <- err }()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatal("canceled End lost its wait cause", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("canceled End blocked behind another End's unrelated waiting context")
	}
	if !span.IsRecording() {
		t.Fatal("canceled waiting ended the live span")
	}
	unblock()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first End did not resume")
	}
	select {
	case <-flushDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Flush did not join")
	}
}
