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
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestTextualConfigurationResidence(t *testing.T) {
	_, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) {})
	options.Name, options.MaxActive, options.MaxConnections = "text-residence", 1, 1
	options.MaxRequestBytes, options.MaxResponseBytes, options.MaxHeaderBytes, options.MaxExchanges = 1, 1, 1024, 1
	options.RootCAPEM = strings.Repeat(options.RootCAPEM, (700<<10)/len(options.RootCAPEM))
	prepared, err := PrepareV1(options)
	if err != nil {
		t.Fatal(err)
	}
	selected := resource.WithLimits(prepared.Select(), prepared.Metadata().Limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "text-residence", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(deadline(t))
	source, _, err := resource.Bind(assembly, selected)
	if err != nil {
		t.Fatal(err)
	}
	if len(source.owner.settings.RootCAPEM) != len(options.RootCAPEM) {
		t.Fatal("resolved owner discarded textual input")
	}
	// resource.Prepared retains the encoded settings, and Select decodes fresh
	// owner settings; their plain PEM bytes alone coexist for the source lifetime.
	minimumOwnedPayload := 2 * int64(len(options.RootCAPEM))
	if prepared.Metadata().SourceBytes < minimumOwnedPayload {
		t.Fatalf("source declaration %d is below its two known retained PEM payloads %d; parsed certificates and transport containers are additional", prepared.Metadata().SourceBytes, minimumOwnedPayload)
	}
}

func TestCanceledTLSCallbackRootLifetime(t *testing.T) {
	origin, options := newPeer(t, false, func(http.ResponseWriter, *http.Request) {})
	config, err := configuredTLS(defaults(options))
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	config.VerifyConnection = func(tls.ConnectionState) error {
		close(entered)
		<-release
		return nil
	}
	options.RootCAPEM = ""
	options.Native.TLS = config
	options.MaxActive, options.MaxConnections = 1, 1
	f := bindFixture(t, options, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type completion struct {
		receipt *invocation.Receipt[Result]
		err     error
	}
	done := make(chan completion, 1)
	cleanup := deadline(t)
	go func() {
		receipt, err := f.client.Do(ctx, cleanup, correlation("tls-callback"), newRequest(t, "GET", origin.URL, nil))
		done <- completion{receipt: receipt, err: err}
	}()
	select {
	case <-entered:
	case <-deadline(t).Done():
		t.Fatal("TLS callback did not start")
	}
	cancel()
	var completed completion
	select {
	case completed = <-done:
	case <-deadline(t).Done():
		t.Fatal("method cancellation did not end the waiter")
	}
	if completed.receipt == nil || !errors.Is(completed.err, context.Canceled) {
		t.Fatal("canceled TLS call did not retain a receipt", completed.err)
	}
	short, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if result, err := completed.receipt.WaitReleased(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("root released while its native TLS callback is blocked: Final=%t Released=%t wait=%v", result.Final, result.Released, err)
	}
	unblock()
	settle(t, f, completed.receipt)
}
