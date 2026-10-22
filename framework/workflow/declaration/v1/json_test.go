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

package declaration

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"
)

func TestStrictJSON(t *testing.T) {
	for _, text := range []string{
		"", `{"a":1,"\u0061":2}`, `{"a":"\ud800"}`, `{"a":"\udc00"}`,
		string([]byte{'"', 0xff, '"'}), "{} {}", "1.0", "1e0", "-0", "9007199254740992",
		"-9007199254740992", "[1,]", `{"a":}`, strings.Repeat("[", 65) + strings.Repeat("]", 65),
	} {
		t.Run(text, func(t *testing.T) {
			if _, err := readJSON(context.Background(), []byte(text), MaxBundleBytes, MaxBundleDepth); err == nil {
				t.Fatalf("invalid JSON admitted: %q", text)
			}
		})
	}
	for _, text := range []string{`{"a":1,"b":"\ud800\udc00"}`, "9007199254740991", "-9007199254740991", "0", "[]", "{}"} {
		if _, err := readJSON(context.Background(), []byte(text), MaxBundleBytes, MaxBundleDepth); err != nil {
			t.Fatal(text, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readJSON(ctx, []byte("{}"), 2, 3); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrCanceled) {
		t.Fatal(err)
	}
	if _, err := readJSON(context.Background(), []byte("{}"), 1, 3); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func TestCanonicalNativeEncoding(t *testing.T) {
	raw := []byte("{\"\\ue000\":2,\"\\ud800\\udc00\":1,\"slash\":\"<>&\\u2028\"}")
	value, err := readJSON(context.Background(), raw, 1024, 8)
	if err != nil {
		t.Fatal(err)
	}
	result, err := canonical(context.Background(), value, 1024)
	if err != nil {
		t.Fatal(err)
	}
	expected := "{\"slash\":\"<>&\u2028\",\"\U00010000\":1,\"\ue000\":2}"
	if string(result) != expected {
		t.Fatalf("%q != %q", result, expected)
	}
	if _, err := canonical(context.Background(), value, 2); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func TestAuthoringBudget(t *testing.T) {
	schema := &DataSchema{Type: "array"}
	schema.Items = schema
	input := Declaration{Workflows: []WorkflowDeclaration{{Spec: Specification{Contracts: map[string]DataSchema{"cycle": *schema}}}}}
	if err := boundAuthoring(context.Background(), input); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	input = Declaration{Workflows: []WorkflowDeclaration{{Namespace: strings.Repeat("a", 4097)}}}
	if err := boundAuthoring(context.Background(), input); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
}

func TestEncodingFailureDuringCancellation(t *testing.T) {
	value := "private-encoding-canary\xff"
	_, baseline := marshal(context.Background(), value, 1024)
	var native *jsontext.SyntacticError
	if !errors.Is(baseline, ErrInvalid) || !errors.As(baseline, &native) {
		t.Fatal("encoding control did not fail with a native syntax error", baseline)
	}
	ctx, stop := cancelAfter(2)
	defer stop()
	raw, err := marshal(ctx, value, 1024)
	if raw != nil || !errors.Is(err, ErrCanceled) || !errors.Is(err, context.Canceled) {
		t.Fatal("encoding cancellation returned data or lost its category", err)
	}
	if !errors.As(err, &native) {
		t.Fatal("cancellation discarded the native encoding failure")
	}
	if _, completed := Diagnose(err); completed || strings.Contains(err.Error(), "canary") {
		t.Fatal("encoding failure changed completion or safe presentation", err)
	}
}
