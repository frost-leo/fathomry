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
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/framework/configuration"
)

func TestVariableLookupIsExplicitCapturedAndIsolated(t *testing.T) {
	type settings struct {
		First  string `json:"first"`
		Second string `json:"second"`
	}
	calls := 0
	lookup := func(name string) configuration.VariableValue {
		if name != "DECLARED" {
			t.Fatal("undeclared lookup")
		}
		calls++
		return configuration.VariableValue{Value: "", Present: true, Source: "dotenv"}
	}
	schema := configuration.Schema[settings]{SchemaVersion: 1, Defaults: settings{First: "default"}}
	bindings := []configuration.Variable{{Name: "DECLARED", Field: "/first", Required: true}, {Name: "DECLARED", Field: "/second"}}
	loaded, err := configuration.LoadVariables(context.Background(), schema, bindings, lookup)
	if err != nil || calls != 1 {
		t.Fatalf("capture: %v, calls %d", err, calls)
	}
	value, err := loaded.Value()
	if err != nil || value.First != "" || value.Second != "" {
		t.Fatal("empty value lost")
	}
	info := loaded.Description()
	want := []configuration.VariableInfo{{Field: "/first", Present: true, Source: "dotenv"}, {Field: "/second", Present: true, Source: "dotenv"}}
	if !reflect.DeepEqual(info.Variables, want) {
		t.Fatal("variable origins lost")
	}
	info.Variables[0].Source = "mutated"
	if loaded.Description().Variables[0].Source != "dotenv" {
		t.Fatal("description aliases private state")
	}
	t.Setenv("DECLARED", "process-value")
	process, err := configuration.LoadVariables(context.Background(), schema, bindings, nil)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := process.Value()
	if actual.First != "process-value" || process.Description().Variables[0].Source != "process" {
		t.Fatal("process default lost")
	}
}

func TestVariableLookupRefusalsAndCancellation(t *testing.T) {
	type settings struct {
		Value string `json:"value"`
	}
	schema := configuration.Schema[settings]{SchemaVersion: 1}
	bindings := []configuration.Variable{{Name: "DECLARED", Field: "/value", Required: true}}
	for _, value := range []configuration.VariableValue{
		{}, {Value: "secret"}, {Source: "invalid/private", Present: true}, {Present: true},
		{Value: strings.Repeat("s", configuration.MaxDocumentBytes+1), Present: true, Source: "dotenv"},
	} {
		loaded, err := configuration.LoadVariables(context.Background(), schema, bindings, func(string) configuration.VariableValue { return value })
		if err == nil || loaded.Description().Revision != "" {
			t.Fatal("invalid lookup accepted")
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatal("private value escaped")
		}
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("intentional cancellation")
	cancel(cause)
	_, err := configuration.LoadVariables(ctx, schema, bindings, func(string) configuration.VariableValue {
		t.Fatal("lookup after cancellation")
		return configuration.VariableValue{}
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation cause lost")
	}
	called := false
	_, err = configuration.LoadVariables(context.Background(), schema, []configuration.Variable{{Name: "DECLARED", Field: "/unknown"}}, func(string) configuration.VariableValue { called = true; return configuration.VariableValue{} })
	if err == nil || called {
		t.Fatal("lookup preceded declaration validation")
	}
}
