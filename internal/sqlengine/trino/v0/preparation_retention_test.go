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

package trino

import (
	"errors"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestPreparationOwnsBoundedNameStorage(t *testing.T) {
	backing := strings.Repeat("name", 1<<18)
	name := backing[:4]
	prepared, err := PrepareV1(OptionsV1{Name: name, Endpoint: "http://127.0.0.1:1", User: "fixture", Plaintext: true})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Options().Name != name || unsafe.StringData(prepared.Options().Name) == unsafe.StringData(name) {
		t.Fatal("resolved source name retained caller backing storage")
	}
	observed := prepared.Description()
	if observed.Identity.Name != name || unsafe.StringData(observed.Identity.Name) == unsafe.StringData(name) {
		t.Fatal("prepared identity retained caller backing storage")
	}
}

func setPreparationText(options *OptionsV1, field, value string) {
	switch field {
	case "endpoint":
		options.Endpoint = value
	case "user":
		options.User = value
	case "password":
		options.Password = value
	case "bearer_token":
		options.BearerToken = value
	case "root_ca_pem":
		options.RootCAPEM = value
	case "catalog":
		options.Catalog = value
	case "schema":
		options.Schema = value
	}
}

func preparationCauseContains(err error, text string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), text) {
		return true
	}
	if single, ok := err.(interface{ Unwrap() error }); ok {
		return preparationCauseContains(single.Unwrap(), text)
	}
	if multiple, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range multiple.Unwrap() {
			if preparationCauseContains(cause, text) {
				return true
			}
		}
	}
	return false
}

func TestPreparationRejectsOversizedTextBeforeCopying(t *testing.T) {
	base, err := PrepareV1(OptionsV1{Name: "bounded", Endpoint: "https://localhost:8443", User: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("x", 8<<20)
	for _, route := range []struct {
		name string
		run  func(OptionsV1) (Preparation, error)
	}{
		{"defaulted", func(value OptionsV1) (Preparation, error) { return PrepareV1(value) }},
		{"resolved", PrepareResolvedV1},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, field := range []string{"endpoint", "user", "password", "bearer_token", "root_ca_pem", "catalog", "schema", "aggregate"} {
				t.Run(field, func(t *testing.T) {
					options := base.Options()
					if field == "aggregate" {
						for _, part := range []string{"endpoint", "user", "password", "bearer_token", "root_ca_pem", "catalog", "schema"} {
							setPreparationText(&options, part, large[:256<<10])
						}
					} else {
						setPreparationText(&options, field, large)
					}
					beforeInput := options
					runtime.GC()
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					_, err := route.run(options)
					runtime.ReadMemStats(&after)
					runtime.KeepAlive(large)
					if !errors.Is(err, resource.ErrConfiguration) || !preparationCauseContains(err, "defaults exceed size limit") {
						t.Fatal("oversized defaults lost their configuration-size error")
					}
					allocated := after.TotalAlloc - before.TotalAlloc
					t.Logf("allocated_bytes=%d", allocated)
					if allocated > 1<<20 {
						t.Fatalf("rejected defaults allocated %d bytes before their size check", allocated)
					}
					if options != beforeInput {
						t.Fatal("preparation mutated caller options")
					}
				})
			}
		})
	}
}

func TestPreparationTextEnvelopePreservesValidationOrder(t *testing.T) {
	base := OptionsV1{Name: "bounded", Endpoint: "https://localhost:8443", User: "fixture"}
	oversized := strings.Repeat("x", (1<<20)+1)
	for _, test := range []struct {
		name   string
		change func(*OptionsV1)
		cause  string
	}{
		{"identity", func(value *OptionsV1) { value.Name = "INVALID"; value.Version = 2 }, "invalid identity"},
		{"format", func(value *OptionsV1) { value.Version = 2; value.Password += "\xff" }, "unsupported configuration format"},
		{"utf8", func(value *OptionsV1) { value.Schema = "\xff" }, "invalid UTF-8 in configuration defaults"},
		{"defaults-before-layers", func(*OptionsV1) {}, "defaults exceed size limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := base
			value.Password = oversized
			test.change(&value)
			_, err := PrepareV1(value, resource.Layer{Kind: resource.Local, Content: []byte("unknown: true")})
			if !errors.Is(err, resource.ErrConfiguration) || !preparationCauseContains(err, test.cause) {
				t.Fatal("configuration rejection precedence changed")
			}
		})
	}
	for _, field := range []string{"endpoint", "user", "password", "bearer_token", "root_ca_pem", "catalog", "schema"} {
		t.Run("repair-"+field, func(t *testing.T) {
			value := base
			setPreparationText(&value, field, strings.Repeat("x", 65537))
			replacement := `""`
			if field == "endpoint" {
				replacement = "https://localhost:8443"
			} else if field == "user" {
				replacement = "fixture"
			}
			if _, err := PrepareV1(value, resource.Layer{Kind: resource.Local, Content: []byte(field + ": " + replacement)}); err != nil {
				t.Fatal("bounded semantic defaults can no longer be repaired by a layer", err)
			}
		})
	}
	for _, escape := range []string{"<", "\u2028"} {
		t.Run("escaped-"+escape, func(t *testing.T) {
			value := base
			value.Password = strings.Repeat(escape, 200000)
			_, err := PrepareV1(value, resource.Layer{Kind: resource.Local, Content: []byte(`password: ""`)})
			if !preparationCauseContains(err, "defaults exceed size limit") {
				t.Fatal("encoded defaults size check was bypassed by a repairing layer")
			}
		})
	}
}
