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

package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/conformance/testdata/facades"
)

const diagnosticCanary = "conformance-private-payload-canary"

type checkCase struct {
	name   string
	reject string
	check  func(*testing.T)
}

func facadeCases() []checkCase {
	var cases []checkCase
	for _, fixture := range []struct {
		name    string
		value   any
		methods []string
		reject  string
	}{
		{"private-state", facades.Safe{}, []string{"Send"}, ""},
		{"private-state-pointer", &facades.Safe{}, []string{"Send"}, ""},
		{"direct-field", facades.Direct{}, []string{"Send"}, "public state"},
		{"direct-state", &facades.State{}, []string{"Send"}, "public state"},
		{"promoted-field", facades.NewPromoted(func() {}), []string{"Send"}, "public state"},
		{"promoted-field-address", new(facades.Promoted), []string{"Send"}, "public state"},
		{"pointer-embedding", facades.NewPointer(func() {}), []string{"Send"}, "public state"},
		{"nil-embedding", facades.Pointer{}, []string{"Send"}, "public state"},
		{"multi-level", facades.NewMulti(func() {}), []string{"Send"}, "public state"},
		{"multi-level-nil", &facades.Multi{}, []string{"Send"}, "public state"},
		{"ambiguous-fields", facades.Ambiguous{}, []string{"Send"}, ""},
		{"diamond", &facades.Diamond{}, []string{"Send"}, ""},
		{"shadowed-field", facades.ShadowedField{}, []string{"Send"}, "public state"},
		{"hidden-path", facades.NewHiddenPath(func() {}), []string{"Send"}, "public state"},
		{"recursive-safe", facades.RecursiveSafe{}, []string{"Send"}, ""},
		{"mutual-safe", &facades.MutualSafe{}, []string{"Send"}, ""},
		{"recursive-nil", facades.Recursive{}, []string{"Send"}, "public state"},
		{"recursive-cycle", facades.NewRecursive(func() {}), []string{"Send"}, "public state"},
		{"promoted-method", facades.PromotedMethod{}, []string{"Send"}, ""},
		{"pointer-method", &facades.PointerMethod{}, []string{"Send"}, ""},
		{"missing-value-method", facades.PointerMethod{}, []string{"Send"}, "method surface"},
		{"nil-method-embedding", facades.NilMethod{}, []string{"Send"}, ""},
		{"owning-method", facades.OwningMethod{}, []string{"Send"}, "method surface"},
		{"owning-pointer-method", facades.PointerOwner{}, []string{"Send"}, "method surface"},
		{"method-shadows-field", facades.MethodShadow{}, []string{"Send", "Shutdown"}, "public state"},
		{"method-field-collision", facades.MethodFieldCollision{}, []string{"Send"}, "public state"},
		{"missing-facade", nil, nil, "missing public facade"},
		{"nil-facade", (*facades.Safe)(nil), []string{"Send"}, "nil public facade"},
		{"empty-method", facades.Safe{}, []string{""}, "invalid facade method allowlist"},
		{"duplicate-method", facades.Safe{}, []string{"Send", "Send"}, "invalid facade method allowlist"},
		{"unlisted-method", facades.Safe{}, nil, "method surface"},
		{"extra-method", facades.Safe{}, []string{"Send", "Read"}, "method surface"},
	} {
		cases = append(cases, checkCase{fixture.name, fixture.reject, func(t *testing.T) {
			conformance.Facade(t, fixture.value, fixture.methods...)
		}})
	}
	return cases
}

func TestFacadeExternalSelectors(t *testing.T) {
	closed := 0
	close := func() { closed++ }
	var capability interface{ Send() } = facades.NewPromoted(close)
	capability.(facades.Promoted).Shutdown()
	pointer := facades.NewPointer(close)
	pointer.Shutdown()
	multi := facades.NewMulti(close)
	multi.Shutdown()
	hidden := facades.NewHiddenPath(close)
	hidden.Shutdown()
	recursive := facades.NewRecursive(close)
	recursive.Shutdown()
	shadow := facades.NewMethodShadow(close)
	shadow.Shutdown()
	if closed != 5 {
		t.Fatal("external selector oracle did not invoke exactly the exposed callbacks")
	}
	// A new defined type removes the outer methods without naming private fields.
	type withoutMethods facades.MethodShadow
	converted := withoutMethods(shadow)
	converted.Shutdown()
	if closed != 6 {
		t.Fatal("ordinary external conversion did not expose the shadowed callback")
	}
	var _ interface{ Send() } = facades.PromotedMethod{}
	var _ interface{ Send() } = &facades.PointerMethod{}
	var _ interface{ Send() } = facades.Ambiguous{}
	var _ interface{ Send() } = facades.Diamond{}
}

func TestFacadeContracts(t *testing.T) { runCheckCases(t, "facade", facadeCases()) }

func runCheckCases(t *testing.T, suite string, cases []checkCase) {
	t.Helper()
	for _, fixture := range cases {
		t.Run(fixture.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHelperContractChild$", "-test.timeout=10s", "-test.v")
			command.Env = append(os.Environ(), "FATHOMRY_HELPER_CASE="+suite+"/"+fixture.name,
				"GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatal("fixture exceeded its execution deadline")
			}
			if bytes.Contains(output, []byte(diagnosticCanary)) {
				t.Fatal("fixture disclosed its private canary")
			}
			if !bytes.Contains(output, []byte("helper fixture completed")) {
				t.Fatal("fixture did not return from the helper contract")
			}
			if fixture.reject == "" {
				if err != nil || bytes.Contains(output, []byte("conformance:")) {
					t.Fatal("valid fixture was rejected")
				}
				return
			}
			var failure *exec.ExitError
			if !errors.As(err, &failure) {
				t.Fatal("invalid fixture passed conformance")
			}
			if failure.ExitCode() != 1 || !bytes.Contains(output, []byte("conformance:")) ||
				!bytes.Contains(output, []byte(fixture.reject)) ||
				!bytes.Contains(output, []byte("--- FAIL: TestHelperContractChild")) {
				t.Fatal("fixture failed without the required conformance diagnostic")
			}
		})
	}
}

func TestHelperContractChild(t *testing.T) {
	selected := os.Getenv("FATHOMRY_HELPER_CASE")
	if selected == "" {
		return
	}
	for suite, cases := range map[string][]checkCase{"facade": facadeCases(), "private": privateCases(), "runtime": runtimeCases()} {
		for _, fixture := range cases {
			if selected == suite+"/"+fixture.name {
				fixture.check(t)
				t.Log("helper fixture completed")
				return
			}
		}
	}
	t.Fatal("unknown helper fixture")
}
