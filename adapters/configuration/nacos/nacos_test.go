/*
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

package nacos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http/httptrace"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/configuration/nacos"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
)

type settings struct {
	Name    string            `json:"name"`
	Large   uint64            `json:"large"`
	Headers map[string]string `json:"headers"`
	Items   []string          `json:"items"`
}

func definition() configuration.Schema[settings] {
	return configuration.Schema[settings]{SchemaVersion: 1, Defaults: settings{Name: "default", Headers: map[string]string{"default": "kept"}}}
}
func provider(t *testing.T, options nacos.Options) *nacos.Provider {
	t.Helper()
	p, err := nacos.New(options)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func assertZero(t *testing.T, loaded configuration.Configuration[settings]) {
	t.Helper()
	if _, err := loaded.Value(); !errors.Is(err, configuration.InvalidInput) || !reflect.DeepEqual(loaded.Description(), configuration.Description{}) {
		t.Fatal("failure published usable settings or metadata")
	}
}

func TestRemoteLayersAndFreshSnapshots(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(fmt.Sprint(secure), func(t *testing.T) {
			t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
			t.Setenv("HTTPS_PROXY", "http://127.0.0.1:1")
			t.Setenv("NO_PROXY", "")
			f := newFixture(t, secure)
			f.mu.Lock()
			f.content["private-local"] = "items: []\nheaders: {Local: added}"
			f.mu.Unlock()
			options := f.options()
			options.Sources = append(options.Sources,
				nacos.Source{Name: "local", DataID: "private-local", Layer: configuration.Local},
				nacos.Source{Name: "environment", DataID: "private-missing", Layer: configuration.Environment, Optional: true})
			p := provider(t, options)
			options.Sources[0].DataID = "changed"
			options.Servers[0].GRPCAddress = "changed"
			if f.queries.Load() != 0 || f.logins.Load() != 0 {
				t.Fatal("constructor performed network I/O")
			}
			t.Setenv("FATHOMRY_REMOTE_NAME", "selected")
			request := configuration.Request{Provider: p, Variables: []configuration.Variable{{Name: "FATHOMRY_REMOTE_NAME", Field: "/name"}}}
			first, err := configuration.Load(context.Background(), definition(), request)
			if err != nil {
				t.Fatal(err)
			}
			value, err := first.Value()
			if err != nil || value.Name != "selected" || value.Large != ^uint64(0) || value.Items == nil || len(value.Items) != 0 ||
				!reflect.DeepEqual(value.Headers, map[string]string{"default": "kept", "X-Tenant": "exact", "Local": "added"}) {
				t.Fatal("raw or layering semantics changed")
			}
			info := first.Description()
			if info.Provider != "nacos" || len(info.Sources) != 3 || info.Sources[1].Present || info.Sources[2].Layer != configuration.Local {
				t.Fatal("source presence/order lost")
			}
			data, _ := json.Marshal(info)
			for _, canary := range []string{"private-base", "private-namespace", "private-password", "private-token", "X-Tenant"} {
				if strings.Contains(string(data), canary) {
					t.Fatal("description disclosed private input")
				}
			}
			value.Headers["default"] = "mutated"
			f.mu.Lock()
			f.content["private-base"] = "name: updated\nlarge: 7"
			f.mu.Unlock()
			second, err := configuration.Load(context.Background(), definition(), request)
			updated, _ := second.Value()
			original, _ := first.Value()
			if err != nil || updated.Large != 7 || original.Large != ^uint64(0) || original.Headers["default"] != "kept" {
				t.Fatal("stale or aliased result")
			}
			if f.queries.Load() != 6 || f.logins.Load() != 2 || f.watches.Load() != 0 {
				t.Fatal("unexpected cache, login lifetime or watch")
			}
		})
	}
}

func TestRemoteRefusalsAreNotOptionalAbsence(t *testing.T) {
	for _, name := range []string{"missing", "denied", "empty", "malformed-document", "unknown-overridden", "malformed-protocol", "encrypted", "oversized", "rpc-deadline"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t, false)
			options := f.options()
			options.Sources = append(options.Sources, nacos.Source{Name: "local", DataID: "private-local", Layer: configuration.Local, Optional: name != "missing"})
			f.mu.Lock()
			f.content["private-local"] = "{}"
			f.faultID = "private-local"
			want := configuration.Invalid
			switch name {
			case "missing":
				delete(f.content, "private-local")
				want = configuration.Unavailable
			case "denied":
				f.fault = 403
				want = configuration.Unavailable
			case "empty":
				f.content["private-local"] = " "
			case "malformed-document":
				f.content["private-local"] = "name: ["
			case "unknown-overridden":
				f.content["private-base"] = "unknown: canary"
				f.content["private-local"] = "name: override"
			case "malformed-protocol":
				f.malformed = true
			case "encrypted":
				f.encrypted = true
			case "oversized":
				f.content["private-local"] = strings.Repeat("x", configuration.MaxDocumentBytes+1)
				want = configuration.LimitExceeded
			case "rpc-deadline":
				f.rpcDeadline = true
				want = configuration.Cancelled
			}
			f.mu.Unlock()
			p := provider(t, options)
			loaded, err := configuration.Load(context.Background(), definition(), configuration.Request{Provider: p})
			if !errors.Is(err, want) {
				t.Fatalf("wrong refusal: %v", err)
			}
			if name == "rpc-deadline" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("native RPC deadline cause was lost")
			}
			if name == "missing" && !errors.Is(err, configuration.Missing) || name == "denied" && !errors.Is(err, configuration.Denied) {
				t.Fatal("safe remote cause lost")
			}
			if want == configuration.Unavailable {
				occurrence, ok := failure.Inspect(err)
				if !ok || !reflect.DeepEqual(occurrence.Diagnostic().Attributes, []failure.Attribute{{Name: "provider", Value: "nacos"}, {Name: "source", Value: "local"}}) {
					t.Fatal("safe failure labels missing")
				}
			}
			assertZero(t, loaded)
			for cause := err; cause != nil; cause = errors.Unwrap(cause) {
				if strings.Contains(fmt.Sprintf("%+v", cause), "private-") {
					t.Fatal("native diagnostics leaked")
				}
			}
			if f.queries.Load() != 2 {
				t.Fatal("positive earlier source was not read")
			}
			if name != "malformed-document" && name != "unknown-overridden" {
				input, err := p.ReadConfiguration(context.Background())
				if !errors.Is(err, want) || !reflect.DeepEqual(input, configuration.Input{}) {
					t.Fatal("adapter returned a usable prefix or metadata on acquisition failure")
				}
			}
		})
	}
}

func TestDeclarationValidationAndCancellation(t *testing.T) {
	f := newFixture(t, false)
	for _, change := range []func(*nacos.Options){
		func(o *nacos.Options) { o.Servers = nil }, func(o *nacos.Options) { o.Sources = nil },
		func(o *nacos.Options) { o.Sources[0].Layer = configuration.Variables },
		func(o *nacos.Options) { o.Sources[0].Name = "private/path" },
		func(o *nacos.Options) { o.Sources[0].DataID = "" },
		func(o *nacos.Options) {
			o.Sources = append(o.Sources, nacos.Source{Name: "other", DataID: "private-base", Group: "DEFAULT_GROUP", Layer: configuration.Local})
		},
		func(o *nacos.Options) { o.Password = "" }, func(o *nacos.Options) { o.AllowInsecure = false },
		func(o *nacos.Options) { o.RootCAPEM = "invalid" }, func(o *nacos.Options) { o.ConcurrentLoads = 17 },
		func(o *nacos.Options) { o.RequestTimeout = time.Nanosecond },
		func(o *nacos.Options) { o.Servers[0].HTTPURL += "?secret" },
	} {
		options := f.options()
		change(&options)
		if p, err := nacos.New(options); p != nil || !errors.Is(err, configuration.InvalidInput) {
			t.Fatal("invalid declaration accepted")
		}
	}
	p := provider(t, f.options())
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller cause")
	cancel(cause)
	input, err := p.ReadConfiguration(ctx)
	if input.Provider != "" || input.Documents != nil || !errors.Is(err, configuration.Cancelled) || !errors.Is(err, cause) {
		t.Fatal("cancelled acquisition escaped")
	}
	for _, p := range []*nacos.Provider{nil, new(nacos.Provider), p} {
		if input, err := p.ReadConfiguration(nil); input.Provider != "" || !errors.Is(err, configuration.InvalidInput) {
			t.Fatal("invalid call accepted")
		}
	}
	if f.queries.Load() != 0 || f.logins.Load() != 0 {
		t.Fatal("rejected preflight performed network I/O")
	}
}

func TestBothTransportAuthoritiesAreVerified(t *testing.T) {
	for _, channel := range []string{"roots", "http", "grpc", "password"} {
		t.Run(channel, func(t *testing.T) {
			f := newFixture(t, true)
			options := f.options()
			switch channel {
			case "roots":
				options.RootCAPEM = ""
			case "http":
				options.Servers[0].HTTPURL = strings.Replace(options.Servers[0].HTTPURL, "127.0.0.1", "localhost", 1)
			case "grpc":
				_, port, _ := net.SplitHostPort(options.Servers[0].GRPCAddress)
				options.Servers[0].GRPCAddress = net.JoinHostPort("localhost", port)
			case "password":
				options.Password = "wrong"
			}
			input, err := provider(t, options).ReadConfiguration(context.Background())
			if !errors.Is(err, configuration.Unavailable) || input.Provider != "" || input.Documents != nil {
				t.Fatal("untrusted transport/authentication accepted")
			}
			if channel == "password" && !errors.Is(err, configuration.Denied) {
				t.Fatal("login denial lost")
			}
			if f.queries.Load() != 0 {
				t.Fatal("untrusted request reached configuration query")
			}
		})
	}
}

func TestCancellationRetainsAdmissionUntilNativeCleanup(t *testing.T) {
	f := newFixture(t, true)
	options := f.options()
	options.ConcurrentLoads = 1
	p := provider(t, options)
	entered, release, done := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	parent, cancel := context.WithCancelCause(context.Background())
	ctx := httptrace.WithClientTrace(parent, &httptrace.ClientTrace{TLSHandshakeStart: func() { entered <- struct{}{}; <-release }})
	result := make(chan error, 1)
	go func() {
		defer close(done)
		input, err := p.ReadConfiguration(ctx)
		if !reflect.DeepEqual(input, configuration.Input{}) {
			err = errors.New("cancelled acquisition returned a usable prefix")
		}
		result <- err
	}()
	t.Cleanup(func() {
		cancel(nil)
		unblock()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("native cleanup did not finish")
		}
	})
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("native TLS hook did not execute")
	}
	cause := errors.New("caller stop")
	cancel(cause)
	select {
	case <-done:
		t.Fatal("returned while native cleanup was still pending")
	case <-time.After(100 * time.Millisecond):
	}
	if _, err := p.ReadConfiguration(context.Background()); !errors.Is(err, configuration.LimitExceeded) {
		t.Fatal("pending cleanup released admission")
	}
	unblock()
	select {
	case err := <-result:
		if !errors.Is(err, configuration.Cancelled) || !errors.Is(err, cause) {
			t.Fatal("caller cancellation lost")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup remained stuck after releasing native work")
	}
	input, err := p.ReadConfiguration(context.Background())
	if err != nil || len(input.Documents) != 1 {
		t.Fatal("cleanup did not release admission for a new independent read", err)
	}
}

func TestOneDeadlineCoversAllSources(t *testing.T) {
	f := newFixture(t, false)
	f.mu.Lock()
	f.delay = 100 * time.Millisecond
	f.content["second"] = "{}"
	f.mu.Unlock()
	options := f.options()
	options.RequestTimeout = 150 * time.Millisecond
	options.Sources = append(options.Sources, nacos.Source{Name: "second", DataID: "second", Layer: configuration.Local})
	input, err := provider(t, options).ReadConfiguration(context.Background())
	if !errors.Is(err, configuration.Cancelled) || !errors.Is(err, context.DeadlineExceeded) || input.Provider != "" || input.Documents != nil {
		t.Fatal("each source received a new total deadline or a prefix escaped", err)
	}
}

func TestNoAmbientCredentialsAndDeclaredSchema(t *testing.T) {
	f := newFixture(t, false)
	f.mu.Lock()
	f.requireAuth = false
	f.mu.Unlock()
	options := f.options()
	options.Username, options.Password, options.SchemaVersion = "", "", 2
	t.Setenv("NACOS_USERNAME", "unselected")
	t.Setenv("NACOS_PASSWORD", "unselected")
	p := provider(t, options)
	input, err := p.ReadConfiguration(context.Background())
	if err != nil || input.SchemaVersion != 2 || f.logins.Load() != 0 {
		t.Fatal("bootstrap used ambient credentials or changed the declared schema", err)
	}
	loaded, err := configuration.Load(context.Background(), definition(), configuration.Request{Provider: p})
	if !errors.Is(err, configuration.UnsupportedSchema) {
		t.Fatal("remote schema mismatch accepted")
	}
	assertZero(t, loaded)
}

func TestAuthorizedMemberFailover(t *testing.T) {
	failed, working := newFixture(t, false), newFixture(t, false)
	options := failed.options()
	options.Servers = append(options.Servers, working.options().Servers[0])
	failed.server.Stop()
	input, err := provider(t, options).ReadConfiguration(context.Background())
	if err != nil || len(input.Documents) != 1 || failed.queries.Load() != 0 || working.queries.Load() != 1 {
		t.Fatal("selected-member failover failed", err)
	}
}

func TestPrivacyAndConcurrentUse(t *testing.T) {
	f := newFixture(t, false)
	options := f.options()
	options.ConcurrentLoads = 4
	p := provider(t, options)
	for _, value := range []any{options, options.Sources[0], options.Servers[0], p} {
		for _, verb := range []string{"%v", "%+v", "%#v"} {
			if strings.Contains(fmt.Sprintf(verb, value), "private-") || strings.Contains(fmt.Sprintf(verb, value), "127.0.0.1") {
				t.Fatal("private bootstrap formatted")
			}
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("test", "value", value)
		if strings.Contains(output.String(), "private-") {
			t.Fatal("private bootstrap logged")
		}
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime serialization accepted")
		}
	}
	if err := json.Unmarshal([]byte("{}"), &options); err == nil || options.Password != "private-password" {
		t.Fatal("runtime reconstruction mutated bootstrap")
	}
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			loaded, err := configuration.Load(context.Background(), definition(), configuration.Request{Provider: p})
			if err != nil {
				t.Error(err)
				return
			}
			value, _ := loaded.Value()
			if value.Large != ^uint64(0) {
				t.Error("concurrent read changed exact data")
			}
		})
	}
	workers.Wait()
	if f.queries.Load() != 4 {
		t.Fatal("independent calls were combined or dropped")
	}
}

func FuzzDeclarations(f *testing.F) {
	f.Add("base", "http://127.0.0.1:8848/nacos", "127.0.0.1:9848", "settings.yaml")
	f.Add("invalid/name", "https://localhost/nacos", "localhost:9848", "")
	f.Fuzz(func(t *testing.T, name, httpURL, address, dataID string) {
		if len(name)+len(httpURL)+len(address)+len(dataID) > 8<<10 {
			return
		}
		p, err := nacos.New(nacos.Options{AllowInsecure: true,
			Servers: []nacos.Server{{HTTPURL: httpURL, GRPCAddress: address}},
			Sources: []nacos.Source{{Name: name, DataID: dataID, Layer: configuration.Base}},
		})
		if err != nil {
			if p != nil || !errors.Is(err, configuration.InvalidInput) {
				t.Fatal("invalid declaration returned usable provider or private failure")
			}
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		input, err := p.ReadConfiguration(ctx)
		if !errors.Is(err, configuration.Cancelled) || input.Provider != "" || input.Documents != nil {
			t.Fatal("cancelled declaration performed acquisition")
		}
	})
}
