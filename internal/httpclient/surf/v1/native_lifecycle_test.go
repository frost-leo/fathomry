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

package surf

import (
	"crypto/tls"
	"io"
	stdhttp "net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enetx/g"
	"github.com/enetx/http/httptrace"
	sdk "github.com/enetx/surf"
)

func TestNativeHTTP3CallbackMustFinishBeforeNativeRelease(t *testing.T) {
	peer := newPeers(t, func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		body := make([]byte, len("payload"))
		if _, err := io.ReadFull(r.Body, body); err != nil || string(body) != "payload" {
			t.Error("native callback fixture body mismatch", err)
		}
		_, _ = io.WriteString(w, "ok")
	}, true)
	client := sdk.NewClient()
	if err := client.FathomrySetTLSConfig(&tls.Config{RootCAs: peer.roots}); err != nil {
		t.Fatal(err)
	}
	builder := client.Builder().Proxy("").ForceHTTP3()
	builder.HTTP3Settings().Set()
	if built := builder.Build(); built.IsErr() {
		t.Fatal(built.Err())
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	ctx := httptrace.WithClientTrace(testContext(t), &httptrace.ClientTrace{WroteRequest: func(httptrace.WroteRequestInfo) {
		close(entered)
		<-release
	}})
	req := client.Post(g.String(peer.tcp.URL)).WithContext(ctx)
	req.GetRequest().Body = io.NopCloser(strings.NewReader("payload"))
	result := req.Do()
	if result.IsErr() {
		t.Fatal(result.Err())
	}
	<-entered
	if body := result.Ok().Body.Bytes(); body.IsErr() {
		t.Fatal(body.Err())
	}
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("native Close returned while callback retained: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if client.FathomryQuiescent() {
		t.Fatal("callback still active but quiescence declared")
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("native close did not finish")
	}
	if !client.FathomryQuiescent() {
		t.Fatal("native release unconfirmed after writer ended")
	}
}
