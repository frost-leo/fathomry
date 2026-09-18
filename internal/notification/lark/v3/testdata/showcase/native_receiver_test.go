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

package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

type nativeProbeLogger struct {
	data atomic.Int32
	seen chan struct{}
	once sync.Once
}

func (logger *nativeProbeLogger) Debug(_ context.Context, args ...interface{}) {
	if len(args) == 1 {
		if nested, ok := args[0].([]interface{}); ok {
			args = nested
		}
	}
	for _, arg := range args {
		if text, ok := arg.(string); ok && strings.HasPrefix(text, "receive message,") {
			logger.data.Add(1)
			logger.once.Do(func() { close(logger.seen) })
			return
		}
	}
}
func (*nativeProbeLogger) Info(context.Context, ...interface{})  {}
func (*nativeProbeLogger) Warn(context.Context, ...interface{})  {}
func (*nativeProbeLogger) Error(context.Context, ...interface{}) {}

// This isolated opt-in comparison intentionally installs NO event handler, so
// the native client cannot send a success ACK or invoke application code.
// Its known cache-worker lifetime is contained by the test process.
func TestLiveNativeWebSocket(t *testing.T) {
	if os.Getenv("FATHOMRY_FEISHU_NATIVE_WEBSOCKET") != "1" {
		t.Skip("requires explicit isolated native receiver comparison")
	}
	options, err := readSettings(os.Getenv("FATHOMRY_FEISHU_CONFIG"))
	if err != nil {
		t.Fatal(err)
	}
	logger := &nativeProbeLogger{seen: make(chan struct{})}
	ready := make(chan struct{})
	var once sync.Once
	client := larkws.NewClient(options.AppID, options.AppSecret, larkws.WithAutoReconnect(false), larkws.WithLogger(logger),
		larkws.WithOnReady(func() { once.Do(func() { close(ready) }) }))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Start(ctx) }()
	select {
	case <-ready:
		t.Log("Official SDK WebSocket connected; no event handler or success ACK is installed.")
	case err := <-done:
		if err != nil {
			t.Fatal("native connection failed")
		}
		return
	case <-ctx.Done():
		t.Fatal("native connection not observed")
	}
	select {
	case <-logger.seen:
	case <-ctx.Done():
	}
	cancel()
	<-done
	data, _ := json.Marshal(struct {
		Connected    bool
		DataMessages int32
	}{true, logger.data.Load()})
	t.Log(string(data))
	if logger.data.Load() == 0 {
		t.Fatal("official SDK also observed no application data message")
	}
}
