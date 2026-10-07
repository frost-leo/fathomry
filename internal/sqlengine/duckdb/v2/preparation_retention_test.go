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

package duckdb

import (
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestPreparationOwnsBoundedNameStorage(t *testing.T) {
	backing := strings.Repeat("name", 1<<18)
	name := backing[:4]
	prepared, err := PrepareV1(OptionsV1{Name: name})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Options().Name != name || unsafe.StringData(prepared.Options().Name) == unsafe.StringData(name) {
		t.Fatal("resolved source name retained caller backing storage")
	}
	observed := prepared.prepared.Description()
	if observed.Identity.Name != name || unsafe.StringData(observed.Identity.Name) == unsafe.StringData(name) {
		t.Fatal("prepared identity retained caller backing storage")
	}
}

func TestPreparationPathBoundsPreserveOverlayAndErrorSemantics(t *testing.T) {
	baseline, err := PrepareV1(OptionsV1{Name: "path-bounds"})
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		name, path  string
		cause       string
		repair      bool
		accepted    bool
		nativeInput bool
	}{
		{name: "maximum-path", path: "/" + strings.Repeat("p", 4095), accepted: true},
		{name: "oversized-path", path: "/" + strings.Repeat("p", 4096), nativeInput: true},
		{name: "repaired-path", path: "/" + strings.Repeat("p", 4096), repair: true, accepted: true},
		{name: "repaired-large-default", path: "/" + strings.Repeat("p", (1<<20)-2049), repair: true, accepted: true},
		{name: "global-bound", path: "/" + strings.Repeat("p", (1<<20)-1)},
		{name: "global-bound-not-repairable", path: "/" + strings.Repeat("p", (1<<20)-1), repair: true},
		{name: "oversized-default", path: "/" + strings.Repeat("p", 1<<20)},
		{name: "oversized-default-not-repairable", path: "/" + strings.Repeat("p", 1<<20), repair: true},
		{name: "oversized-invalid-utf8", path: "/" + strings.Repeat("p", 1<<20) + "\xff", cause: "source: invalid UTF-8 in configuration defaults"},
		{name: "oversized-invalid-utf8-not-repairable", path: "/" + strings.Repeat("p", 1<<20) + "\xff", repair: true, cause: "source: invalid UTF-8 in configuration defaults"},
		{name: "invalid-utf8", path: "/" + strings.Repeat("\xff", 4096)},
		{name: "invalid-utf8-not-repairable", path: "/" + strings.Repeat("\xff", 4096), repair: true},
		{name: "escaped-global-bound", path: "/" + strings.Repeat("<", 200000)},
		{name: "escaped-global-bound-not-repairable", path: "/" + strings.Repeat("<", 200000), repair: true},
		{name: "escaped-repairable", path: "/" + strings.Repeat("<", 10000), repair: true, accepted: true},
		{name: "unicode-global-bound", path: "/" + strings.Repeat("\u2028", 200000)},
		{name: "unicode-global-bound-not-repairable", path: "/" + strings.Repeat("\u2028", 200000), repair: true},
		{name: "unicode-repairable", path: "/" + strings.Repeat("\u2028", 10000), repair: true, accepted: true},
	} {
		t.Run(check.name, func(t *testing.T) {
			selected := baseline.Options()
			selected.Path = check.path
			before := selected
			var layers []resource.Layer
			if check.repair {
				layers = []resource.Layer{{Kind: resource.Local, Content: []byte(`{"path":""}`)}}
			}
			verify := func(prepared Preparation, err error) {
				t.Helper()
				if selected != before {
					t.Fatal("preparation mutated caller options")
				}
				if check.accepted {
					wantPath := check.path
					if check.repair {
						wantPath = ""
					}
					if err != nil || prepared.Options().Path != wantPath || prepared.Budget() != baseline.Budget() {
						t.Fatal("valid path or repairing overlay changed", err)
					}
					return
				}
				if !errors.Is(err, resource.ErrConfiguration) || errors.Is(err, ErrInput) != check.nativeInput || prepared.Budget() != (Budget{}) {
					t.Fatal("path refusal lost its configuration/native identity", err)
				}
				var technical *fault.Error
				if !errors.As(err, &technical) || technical.Diagnostic().Context != (fault.Context{Operation: "prepare", Provider: ProviderID, Source: selected.Name}) {
					t.Fatal("path refusal changed resource-owned diagnostic context")
				}
				if check.cause != "" {
					causes := technical.Unwrap()
					if len(causes) != 1 || causes[0].Error() != check.cause {
						t.Fatal("oversized preparation changed the original validation cause")
					}
				}
			}
			verify(PrepareV1(selected, layers...))
			if !check.repair {
				verify(PrepareResolvedV1(selected))
			}
		})
	}
}

func TestOversizedPreparationPreservesBoundedIdentity(t *testing.T) {
	path := "/" + strings.Repeat("p", (1<<20)+1)
	for _, check := range []struct{ name, want string }{
		{"bounded-name", "bounded-name"},
		{"INVALID", ""},
		{"", ""},
		{strings.Repeat("name", 1<<19), ""},
	} {
		for _, invalidUTF8 := range []bool{false, true} {
			selected := OptionsV1{Name: check.name, Path: path}
			if invalidUTF8 {
				selected.Path += "\xff"
			}
			before := selected
			prepared, err := PrepareV1(selected)
			var technical *fault.Error
			if !errors.Is(err, resource.ErrConfiguration) || errors.Is(err, ErrInput) || !errors.As(err, &technical) || prepared.Budget() != (Budget{}) {
				t.Fatal("oversized defaults changed failure classification", err)
			}
			location := technical.Diagnostic().Context
			if !location.Valid() || location.Source != check.want || location.Provider != ProviderID || location.Operation != "prepare" {
				t.Fatal("oversized defaults retained invalid or unbounded identity")
			}
			cause := "source: defaults exceed size limit"
			if check.want == "" {
				cause = "source: invalid identity"
			} else if invalidUTF8 {
				cause = "source: invalid UTF-8 in configuration defaults"
			}
			if causes := technical.Unwrap(); len(causes) != 1 || causes[0].Error() != cause {
				t.Fatal("identity, UTF-8 and size validation priority changed")
			}
			if selected != before {
				t.Fatal("preparation mutated oversized caller settings")
			}
		}
	}
}
