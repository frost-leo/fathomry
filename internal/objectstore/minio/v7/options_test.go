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

package minio

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestConfigurationValidationNoIO(t *testing.T) {
	server, options := newPeer(t)
	for _, test := range []struct {
		name   string
		change func(*OptionsV1)
	}{
		{"name", func(v *OptionsV1) { v.Name = "" }}, {"format", func(v *OptionsV1) { v.Version = 2 }},
		{"dns", func(v *OptionsV1) { v.Endpoint = "http://localhost:9000" }}, {"credentials", func(v *OptionsV1) { v.AccessKey = "" }},
		{"region", func(v *OptionsV1) { v.Region = "" }}, {"bucket", func(v *OptionsV1) { v.Bucket = "bad/bucket" }},
		{"ambient-tls", func(v *OptionsV1) { v.Plaintext = false }}, {"roots", func(v *OptionsV1) { v.RootCAPEM = "secret-canary" }},
		{"embedded-auth", func(v *OptionsV1) { v.Endpoint = "http://secret@127.0.0.1:9000" }},
		{"endpoint-path", func(v *OptionsV1) { v.Endpoint += "/prefix" }}, {"query", func(v *OptionsV1) { v.Endpoint += "?private=x" }},
		{"empty-query", func(v *OptionsV1) { v.Endpoint += "?" }},
		{"empty-fragment", func(v *OptionsV1) { v.Endpoint += "#" }},
		{"prefix-traversal", func(v *OptionsV1) { v.Prefix = "owned/../" }},
		{"part", func(v *OptionsV1) { v.PartBytes = 1 }}, {"workers", func(v *OptionsV1) { v.MaxActive = 17 }},
		{"negative-queue", func(v *OptionsV1) { v.QueuedCalls = -1 }}, {"parts", func(v *OptionsV1) { v.MaxParts = 1001 }},
		{"capacity", func(v *OptionsV1) { v.MaxTransferBytes = 1 << 40 }}, {"entries", func(v *OptionsV1) { v.MaxEntries = 1001 }},
		{"directory-bucket", func(v *OptionsV1) { v.Bucket = "example--usw2-az1--x-s3" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := options
			test.change(&value)
			if _, err := Select(value); !errors.Is(err, resource.ErrConfiguration) {
				t.Fatal("invalid configuration accepted", err)
			}
		})
	}
	for _, layer := range []string{"concurrent_stream_parts: true", "max_active: 0", "region: null", "max_requests: 0", "timeout_ns: 0", "max_entries: 1\nmax_entries: 2"} {
		if _, err := Select(options, resource.Layer{Kind: resource.Local, Content: []byte(layer)}); err == nil {
			t.Fatal("invalid/unsupported layer accepted")
		}
	}
	if server.count() != 0 {
		t.Fatal("configuration validation performed I/O")
	}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	if assembly, err := resource.Assemble(deadline(t), deadline(t), "duplicate", selected, selected); err == nil || assembly != nil || server.count() != 0 {
		t.Fatal("duplicate preflight ran construction")
	}
}
func TestPreparationBorrowingAndSourceIsolation(t *testing.T) {
	server, options := newPeer(t)
	layer := []byte("max_entries: 3")
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: layer})
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	layer[0] = 'X'
	options.SecretKey = "mutated"
	options.Prefix = "elsewhere/"
	assembly, err := resource.Assemble(deadline(t), deadline(t), "owner", selected)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	})
	inbox, _ := invocation.NewInbox[Result](4, 256<<20)
	client, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if client.owner.settings.SecretKey == "mutated" || client.owner.settings.Prefix != "owned/" || client.owner.settings.MaxEntries != 3 {
		t.Fatal("prepared state aliased")
	}
	alias := resource.Borrow("alias", assembly, selected)
	borrowed, err := resource.Assemble(deadline(t), deadline(t), "borrower", alias)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := Bind(borrowed, alias, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if bound.owner != client.owner {
		t.Fatal("borrowing duplicated ownership")
	}
	if err := borrowed.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := bound.Stat(deadline(t), correlation("closed-alias"), Address{Key: "owned/x"}); err == nil {
		t.Fatal("closed alias usable")
	}
	before := server.count()
	secondOptions := options
	secondOptions.Name = "separate"
	secondOptions.AccessKey = "second-access"
	secondOptions.Prefix = "owned/"
	second := bindFixture(t, secondOptions, 4)
	if second.client.owner == client.owner {
		t.Fatal("independent selection shared runtime")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.requests) != before+1 || !strings.Contains(server.requests[len(server.requests)-1].header.Get("Authorization"), "Credential=second-access/") {
		t.Fatal("credential/source identity leaked")
	}
}
func TestCookiesAndUnownedContextsCannotBypassSource(t *testing.T) {
	server, options := newPeer(t)
	fixture := bindFixture(t, options, 4)
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if request.Method == "HEAD" {
			writer.Header().Add("Set-Cookie", "private=canary; Path=/")
			objectHeaders(writer, storedObject{body: []byte{}, version: "v1"})
			return true
		}
		return false
	}
	server.mu.Unlock()
	for range 2 {
		receipt, err := fixture.client.Stat(deadline(t), correlation("cookie"), Address{Key: "owned/x"})
		if result := settle(t, receipt, err); result.Err() != nil {
			t.Fatal(result.Err())
		}
	}
	server.mu.Lock()
	for _, request := range server.requests {
		if request.header.Get("Cookie") != "" {
			t.Error("native cookie jar retained unbounded ambient state")
		}
	}
	server.mu.Unlock()
	request, _ := http.NewRequestWithContext(context.Background(), "GET", options.Endpoint+"/fixture/owned/x", nil)
	if _, err := fixture.client.owner.wire.RoundTrip(request); !errors.Is(err, ErrAuthority) {
		t.Fatal("unowned native request accepted")
	}
}

func TestSecondReadinessFailureRetainsNativeCauseAndCleanup(t *testing.T) {
	server, options := newPeer(t)
	first, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	first = resource.WithLimits(first, LimitsV1(options))
	options.Name = "second"
	options.AccessKey = "denied-access"
	second, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	second = resource.WithLimits(second, LimitsV1(options))
	server.mu.Lock()
	server.hook = func(writer http.ResponseWriter, request *http.Request) bool {
		if strings.Contains(request.Header.Get("Authorization"), "Credential=denied-access/") {
			errorResponse(writer, 403, "AccessDenied")
			return true
		}
		return false
	}
	server.mu.Unlock()
	assembly, err := resource.Assemble(deadline(t), deadline(t), "partial", first, second)
	if !errors.Is(err, ErrConnect) || assembly == nil || assembly.Snapshot().Ready {
		t.Fatal("failed assembly became usable or lost cleanup owner")
	}
	if err := assembly.Close(deadline(t)); err != nil {
		t.Fatal(err)
	}
	for _, source := range assembly.Snapshot().Sources {
		if !source.Released || !source.Quiescent {
			t.Fatal("partially initialized transport remains owned without a report")
		}
	}
}
