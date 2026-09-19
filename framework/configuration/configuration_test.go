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

package configuration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
)

type providerFunc func(context.Context) (configuration.Input, error)

func (function providerFunc) ReadConfiguration(ctx context.Context) (configuration.Input, error) {
	return function(ctx)
}

type connection struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}
type settings struct {
	Connection connection        `json:"connection"`
	Headers    map[string]string `json:"headers"`
	Items      []string          `json:"items"`
	Optional   *connection       `json:"optional"`
	Enabled    bool              `json:"enabled"`
	Large      uint64            `json:"large"`
}

func schema() configuration.Schema[settings] {
	return configuration.Schema[settings]{SchemaVersion: 1, Defaults: settings{
		Connection: connection{Host: "default", Port: 443}, Headers: map[string]string{"Default": "kept"},
		Items: []string{"default"}, Optional: &connection{Host: "default", Port: 443}, Enabled: true,
	}}
}
func input(raw string) configuration.Input {
	return configuration.Input{Provider: "test.source", SchemaVersion: 1, Documents: []configuration.Document{
		{Name: "base", Layer: configuration.Base, Data: []byte(raw)},
	}}
}
func provider(value configuration.Input) providerFunc {
	return func(context.Context) (configuration.Input, error) { return value, nil }
}
func checkZero[T any](t *testing.T, value configuration.Configuration[T]) {
	t.Helper()
	if !reflect.DeepEqual(value.Description(), configuration.Description{}) {
		t.Fatal("failed load retained a description")
	}
	if _, err := value.Value(); !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("failed load retained usable settings")
	}
}

func TestLayersCopiesAndProvenance(t *testing.T) {
	t.Setenv("FATHOMRY_TEST_HOST", "")
	t.Setenv("FATHOMRY_TEST_ENABLED", "false")
	t.Setenv("FATHOMRY_TEST_LARGE", "18446744073709551615")
	definition := schema()
	definition.Validate = func(value settings) error { value.Headers["Default"] = "validator"; value.Optional = nil; return nil }
	source := configuration.Input{Provider: "test.source", SchemaVersion: 1, Documents: []configuration.Document{
		{Name: "local", Layer: configuration.Local, Data: []byte("items: []\noptional: null\nheaders: {Local: ''}")},
		{Name: "base", Layer: configuration.Base, Data: []byte("connection: {host: base}\nheaders: {X-Tenant: account}")},
		{Name: "environment", Layer: configuration.Environment, Data: []byte("connection: {port: 8443}")},
	}}
	request := configuration.Request{Provider: provider(source), Variables: []configuration.Variable{
		{Name: "FATHOMRY_TEST_HOST", Field: "/connection/host"},
		{Name: "FATHOMRY_TEST_ENABLED", Field: "/enabled", Encoding: configuration.VariableJSON},
		{Name: "FATHOMRY_TEST_LARGE", Field: "/large", Encoding: configuration.VariableJSON},
	}}
	loaded, err := configuration.Load(context.Background(), definition, request)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := loaded.Value()
	if err != nil || actual.Connection != (connection{Host: "", Port: 8443}) || actual.Enabled ||
		actual.Items == nil || len(actual.Items) != 0 || actual.Optional != nil || actual.Large != ^uint64(0) ||
		!reflect.DeepEqual(actual.Headers, map[string]string{"Default": "kept", "Local": "", "X-Tenant": "account"}) {
		t.Fatal("layer or exact-value semantics changed")
	}
	original := loaded.Description()
	if original.SchemaVersion != 1 || original.Provider != "test.source" || original.Revision == "" ||
		len(original.Sources) != 3 || original.Sources[0].Layer != configuration.Base || len(original.Contributions) != 5 {
		t.Fatalf("wrong safe description: %+v", original)
	}
	for _, contribution := range original.Contributions {
		for _, field := range contribution.Fields {
			if strings.Contains(field, "X-Tenant") {
				t.Fatal("dynamic map key exposed as provenance")
			}
		}
	}
	actual.Headers["Default"] = "caller"
	definition.Defaults.Headers["Default"] = "changed-default"
	source.Documents[0].Data[0] = 'x'
	description := loaded.Description()
	description.Sources[0].Name = "changed"
	description.Contributions[0].Fields[0] = "changed"
	description.Variables[0].Field = "changed"
	t.Setenv("FATHOMRY_TEST_HOST", "later")
	next, _ := loaded.Value()
	if next.Headers["Default"] != "kept" || next.Connection.Host != "" || !reflect.DeepEqual(original, loaded.Description()) {
		t.Fatal("loaded value or metadata aliased caller state")
	}
}

func TestRejectsMalformedAndOverriddenLayers(t *testing.T) {
	cases := []string{
		"", "[]", "null", "unknown: secret-canary", "connection: {port: 65536}", "connection: {port: 1e2}",
		"enabled: null", "enabled: yes", "enabled: true\nenabled: false", "enabled: !!bool true",
		"items: &a [x]", "headers: {<<: {x: y}}", "{}\n---\n{}", "large: 9007199254740993.0",
		"connection: {host: 2026-09-19}", "headers: {x: .nan}", "connection: {host: x}\nConnection: {}",
	}
	for index, raw := range cases {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			source := input(raw)
			source.Documents = append(source.Documents, configuration.Document{Name: "local", Layer: configuration.Local, Data: []byte("connection: {port: 80}\nenabled: false")})
			loaded, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(source)})
			if !errors.Is(err, configuration.Invalid) || strings.Contains(fmt.Sprintf("%+v", err), "secret-canary") {
				t.Fatalf("malformed input accepted or disclosed: %v", err)
			}
			checkZero(t, loaded)
		})
	}
}

func TestInputAndProviderFailures(t *testing.T) {
	for _, mutate := range []func(*configuration.Input){
		func(value *configuration.Input) { value.Provider = "private/path" },
		func(value *configuration.Input) { value.Documents[0].Name = "secret\nname" },
		func(value *configuration.Input) { value.Documents[0].Layer = configuration.Variables },
		func(value *configuration.Input) { value.Documents[0].Absent = true },
		func(value *configuration.Input) { value.Documents = append(value.Documents, value.Documents[0]) },
	} {
		source := input("{}")
		mutate(&source)
		value, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(source)})
		if !errors.Is(err, configuration.InvalidInput) {
			t.Fatal("invalid provider output accepted")
		}
		checkZero(t, value)
	}
	source := input(strings.Repeat("x", configuration.MaxDocumentBytes+1))
	value, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(source)})
	if !errors.Is(err, configuration.LimitExceeded) {
		t.Fatal("oversized source accepted")
	}
	checkZero(t, value)
	native := errors.New("private-native-canary")
	value, err = configuration.Load(context.Background(), schema(), configuration.Request{Provider: providerFunc(func(context.Context) (configuration.Input, error) { return input("large: 7"), native })})
	if !errors.Is(err, configuration.Unavailable) || errors.Is(err, native) || strings.Contains(fmt.Sprintf("%+v", err), "canary") {
		t.Fatal("native error or partial input escaped")
	}
	checkZero(t, value)
	public := failure.New(failure.Code("example.source.unavailable"), nil)
	value, err = configuration.Load(context.Background(), schema(), configuration.Request{Provider: providerFunc(func(context.Context) (configuration.Input, error) { return input("{}"), public })})
	if err != public {
		t.Fatal("public source failure was reclassified")
	}
	checkZero(t, value)
}

func TestCancellationValidationAndNilInputs(t *testing.T) {
	var calls int
	reader := providerFunc(func(ctx context.Context) (configuration.Input, error) {
		calls++
		if ctx == nil {
			t.Fatal("context missing")
		}
		return input("{}"), nil
	})
	var typedNil providerFunc
	for _, request := range []configuration.Request{{}, {Provider: typedNil}} {
		value, err := configuration.Load(context.Background(), schema(), request)
		if !errors.Is(err, configuration.InvalidInput) {
			t.Fatal("nil provider accepted")
		}
		checkZero(t, value)
	}
	if _, err := configuration.Load(nil, schema(), configuration.Request{Provider: reader}); !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("nil Context accepted")
	}
	bad := schema()
	bad.SchemaVersion = 0
	if _, err := configuration.Load(context.Background(), bad, configuration.Request{Provider: reader}); !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("zero schema accepted")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("caller-owned-cause")
	cancel(cause)
	value, err := configuration.Load(ctx, schema(), configuration.Request{Provider: reader})
	if !errors.Is(err, configuration.Cancelled) || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || calls != 0 {
		t.Fatal("cancellation/preflight contract changed")
	}
	checkZero(t, value)
	validation := errors.New("caller-owned-validation")
	definition := schema()
	definition.Validate = func(settings) error { return validation }
	value, err = configuration.Load(context.Background(), definition, configuration.Request{Provider: reader})
	if !errors.Is(err, configuration.ValidationFailed) || !errors.Is(err, validation) {
		t.Fatal("intentional validator cause lost")
	}
	checkZero(t, value)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	definition.Validate = func(settings) error { stop(); return nil }
	value, err = configuration.Load(ctx, definition, configuration.Request{Provider: reader})
	if !errors.Is(err, configuration.Cancelled) {
		t.Fatal("cancelled validation published settings")
	}
	checkZero(t, value)
}

type cancelAfterObservation struct {
	context.Context
	cancel context.CancelFunc
	armed  bool
}

func (ctx *cancelAfterObservation) Err() error {
	observed := ctx.Context.Err()
	if ctx.armed {
		ctx.armed = false
		ctx.cancel()
	}
	return observed
}

func TestCancellationAtPhaseBoundaries(t *testing.T) {
	for _, phase := range []string{"acquisition", "preparation"} {
		t.Run(phase, func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cancelAfterObservation{Context: parent, cancel: cancel, armed: phase == "acquisition"}
			acquired, validated := false, false
			definition := schema()
			definition.Validate = func(settings) error {
				validated = true
				return nil
			}
			reader := providerFunc(func(context.Context) (configuration.Input, error) {
				acquired = true
				ctx.armed = phase == "preparation"
				return input("{}"), nil
			})
			loaded, err := configuration.Load(ctx, definition, configuration.Request{Provider: reader})
			if !errors.Is(err, configuration.Cancelled) || !errors.Is(err, context.Canceled) {
				t.Fatal("cancellation was not preserved")
			}
			if validated || phase == "acquisition" && acquired {
				t.Fatal("a later phase started after cancellation was already available")
			}
			if phase == "preparation" && !acquired {
				t.Fatal("the positive acquisition control did not execute")
			}
			checkZero(t, loaded)
		})
	}
}

func TestEnvironmentPolicy(t *testing.T) {
	t.Setenv("FATHOMRY_TEST_SELECTED", "captured")
	reader := providerFunc(func(context.Context) (configuration.Input, error) {
		if err := os.Setenv("FATHOMRY_TEST_SELECTED", "changed-during-acquisition"); err != nil {
			t.Fatal(err)
		}
		return input("{}"), nil
	})
	request := configuration.Request{Provider: reader, Variables: []configuration.Variable{{Name: "FATHOMRY_TEST_SELECTED", Field: "/connection/host"}}}
	value, err := configuration.Load(context.Background(), schema(), request)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := value.Value()
	if actual.Connection.Host != "captured" {
		t.Fatal("environment was re-read after acquisition")
	}
	value, err = configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(input("{}"))})
	actual, _ = value.Value()
	if err != nil || actual.Connection.Host != "default" {
		t.Fatal("undeclared environment changed settings")
	}
	t.Setenv("FATHOMRY_TEST_SELECTED", "null")
	value, err = configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(input("{}")), Variables: request.Variables})
	actual, _ = value.Value()
	if err != nil || actual.Connection.Host != "null" {
		t.Fatal("literal text was interpreted as JSON")
	}
	for _, binding := range []configuration.Variable{
		{Name: "", Field: "/enabled"}, {Name: "9INVALID", Field: "/enabled"}, {Name: "FATHOMRY_TEST_SELECTED", Field: "/unknown"},
		{Name: "FATHOMRY_TEST_SELECTED", Field: "/headers/dynamic"}, {Name: "FATHOMRY_TEST_SELECTED", Field: "/items/0"},
		{Name: "FATHOMRY_TEST_SELECTED", Field: "connection/host"}, {Name: "FATHOMRY_TEST_SELECTED", Field: "/enabled", Encoding: 99},
	} {
		called := false
		read := providerFunc(func(context.Context) (configuration.Input, error) { called = true; return input("{}"), nil })
		value, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: read, Variables: []configuration.Variable{binding}})
		if !errors.Is(err, configuration.InvalidInput) || called {
			t.Fatal("invalid binding was not rejected before acquisition")
		}
		checkZero(t, value)
	}
	for _, bindings := range [][]configuration.Variable{
		{{Name: "FATHOMRY_TEST_SELECTED", Field: "/connection"}, {Name: "FATHOMRY_TEST_SELECTED", Field: "/connection/host"}},
		{{Name: "FATHOMRY_TEST_SELECTED", Field: "/enabled"}, {Name: "FATHOMRY_TEST_SELECTED", Field: "/enabled"}},
		{{Name: "FATHOMRY_TEST_MISSING_79", Field: "/enabled", Required: true}},
	} {
		t.Setenv("FATHOMRY_TEST_MISSING_79", "")
		if err := os.Unsetenv("FATHOMRY_TEST_MISSING_79"); err != nil {
			t.Fatal(err)
		}
		value, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(input("{}")), Variables: bindings})
		if !errors.Is(err, configuration.InvalidInput) {
			t.Fatal("ambiguous/required binding accepted")
		}
		checkZero(t, value)
	}
	t.Setenv("FATHOMRY_TEST_SELECTED", "")
	request = configuration.Request{Provider: provider(input("{}")), Variables: []configuration.Variable{{Name: "FATHOMRY_TEST_SELECTED", Field: "/enabled", Encoding: configuration.VariableJSON}}}
	if _, err := configuration.Load(context.Background(), schema(), request); !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("empty JSON accepted")
	}
}

func TestRecursiveBindingTypeRefuses(t *testing.T) {
	type recursive *recursive
	type invalid struct {
		Value recursive `json:"value"`
	}
	_, err := configuration.Load(context.Background(), configuration.Schema[invalid]{SchemaVersion: 1}, configuration.Request{
		Provider: provider(input("{}")), Variables: []configuration.Variable{{Name: "FATHOMRY_TEST_MISSING_79", Field: "/value/next"}},
	})
	if !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("recursive pointer binding accepted")
	}
}

func TestPrivacyAndSerializationRefusal(t *testing.T) {
	const canary = "private-configuration-canary"
	value, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(input("connection: {host: " + canary + "}"))})
	if err != nil {
		t.Fatal(err)
	}
	values := []any{
		configuration.Schema[settings]{SchemaVersion: 1, Defaults: settings{Connection: connection{Host: canary}}},
		configuration.Input{Provider: canary, Documents: []configuration.Document{{Data: []byte(canary)}}},
		configuration.Document{Name: canary, Data: []byte(canary)},
		configuration.Variable{Name: canary, Field: canary},
		configuration.Request{Variables: []configuration.Variable{{Name: canary}}},
		value,
	}
	for _, subject := range values {
		if _, ok := subject.(fmt.Formatter); !ok {
			t.Fatalf("runtime type lost formatter: %T", subject)
		}
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
			if strings.Contains(fmt.Sprintf(verb, subject), canary) {
				t.Fatalf("runtime data escaped via %s", verb)
			}
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("test", "value", subject)
		if strings.Contains(output.String(), canary) {
			t.Fatal("runtime data escaped logging")
		}
		if _, err := json.Marshal(subject); err == nil {
			t.Fatal("runtime JSON persistence accepted")
		}
	}
	input := configuration.Input{Provider: "original", SchemaVersion: 1}
	if err := json.Unmarshal([]byte("null"), &input); err == nil || input.Provider != "original" {
		t.Fatal("runtime reconstruction mutated input")
	}
	description, err := json.Marshal(value.Description())
	if err != nil || strings.Contains(string(description), canary) {
		t.Fatal("safe description disclosed values")
	}
}

func TestConcurrentIsolation(t *testing.T) {
	source := provider(input("headers: {X-Tenant: stable}"))
	var validations atomic.Int64
	definition := schema()
	definition.Validate = func(settings) error { validations.Add(1); return nil }
	var wait sync.WaitGroup
	for range 16 {
		wait.Go(func() {
			for range 8 {
				loaded, err := configuration.Load(context.Background(), definition, configuration.Request{Provider: source})
				if err != nil {
					t.Error(err)
					return
				}
				value, _ := loaded.Value()
				value.Headers["X-Tenant"] = "local-mutation"
				other, _ := loaded.Value()
				if other.Headers["X-Tenant"] != "stable" {
					t.Error("shared value mutated")
				}
			}
		})
	}
	wait.Wait()
	if validations.Load() != 128 {
		t.Fatal("validation count changed")
	}
}

func TestSchemaEvolutionIsExplicit(t *testing.T) {
	type original struct {
		Timeout int64 `json:"timeout_seconds"`
	}
	type revised struct {
		Timeout int64 `json:"timeout_milliseconds"`
	}
	oldInput := input("timeout_seconds: 2")
	old, err := configuration.Load(context.Background(), configuration.Schema[original]{SchemaVersion: 1}, configuration.Request{Provider: provider(oldInput)})
	if err != nil {
		t.Fatal(err)
	}
	value, err := configuration.Load(context.Background(), configuration.Schema[revised]{SchemaVersion: 2}, configuration.Request{Provider: provider(oldInput)})
	if !errors.Is(err, configuration.UnsupportedSchema) {
		t.Fatal("old data silently interpreted as a new schema")
	}
	checkZero(t, value)
	oldValue, _ := old.Value()
	converted := revised{Timeout: oldValue.Timeout * 1000}
	encoded, err := json.Marshal(converted)
	if err != nil {
		t.Fatal(err)
	}
	newInput := input(string(encoded))
	newInput.SchemaVersion = 2
	updated, err := configuration.Load(context.Background(), configuration.Schema[revised]{SchemaVersion: 2}, configuration.Request{Provider: provider(newInput)})
	got, _ := updated.Value()
	if err != nil || got.Timeout != 2000 {
		t.Fatal("explicit business conversion did not preserve units")
	}
	again, _ := old.Value()
	if again.Timeout != 2 {
		t.Fatal("new preparation modified old configuration")
	}
	if old.Description().Revision == updated.Description().Revision {
		t.Fatal("preparations reused revision")
	}
}

func FuzzLoad(f *testing.F) {
	for _, seed := range []string{"{}", "large: 18446744073709551615", "enabled: null", "unknown: value"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 32<<10 {
			t.Skip()
		}
		loaded, err := configuration.Load(context.Background(), schema(), configuration.Request{Provider: provider(input(raw))})
		if err != nil {
			checkZero(t, loaded)
			return
		}
		first, _ := loaded.Value()
		second, _ := loaded.Value()
		if !reflect.DeepEqual(first, second) {
			t.Fatal("unstable prepared data")
		}
		info := loaded.Description()
		if info.SchemaVersion != 1 || !slices.ContainsFunc(info.Contributions, func(item configuration.Contribution) bool { return item.Layer == configuration.Base }) {
			t.Fatal("missing source provenance")
		}
	})
}
