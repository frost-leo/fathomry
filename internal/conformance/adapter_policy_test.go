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
	"context"
	"errors"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPublicAdapterPolicyResponsibilityControls(t *testing.T) {
	const lark = "notification/lark/v1"
	type control struct {
		name, provider, file, remove, add, diagnostic string
		misplaced                                     bool
	}
	controls := []control{{name: "valid"}}
	for _, declaration := range []struct{ file, symbol, source string }{
		{"options.go", "Settings", "type Settings struct{}\n"},
		{"options.go", "Dependencies", "type Dependencies struct{}\n"},
		{"options.go", "Validate", "func Validate() {}\n"},
		{"policy.go", "Recommend", "func Recommend() {}\n"},
	} {
		for _, misplaced := range []bool{false, true} {
			name := "missing_"
			if misplaced {
				name = "misplaced_"
			}
			controls = append(controls, control{
				name: "lark_" + name + declaration.symbol, provider: lark,
				file: declaration.file, remove: declaration.source, misplaced: misplaced,
				diagnostic: declaration.file + " must keep " + declaration.symbol + " separately reviewable",
			})
		}
	}
	for _, file := range []string{"options.go", "policy.go"} {
		for _, symbol := range []string{"Open", "Using", "DisableNativeLogging", "NewPassword"} {
			controls = append(controls, control{
				name: "lark_" + file + "_" + symbol, provider: lark, file: file,
				add:        "func " + symbol + "() {}\n",
				diagnostic: file + " mixes configuration/policy with live authority " + symbol,
			})
		}
	}
	controls = append(controls,
		control{name: "future_v2_without_catalog", provider: "future/native/v2", file: "policy.go",
			remove: "func Recommend() {}\n", diagnostic: "policy.go must keep Recommend separately reviewable"},
		control{name: "temporal_singular_options", provider: "orchestration/temporal/v1", file: "option.go",
			remove: "func Validate() {}\n", diagnostic: "option.go must keep Validate separately reviewable"},
		control{name: "method_is_not_recommend", provider: lark, file: "policy.go",
			remove: "func Recommend() {}\n", add: "type recommender struct{}\nfunc (recommender) Recommend() {}\n",
			diagnostic: "policy.go must keep Recommend separately reviewable"},
	)
	for _, test := range controls {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			optionsFiles := map[string]string{
				lark:                        "options.go",
				"orchestration/temporal/v1": "option.go",
				"future/native/v1":          "options.go",
				"future/native/v2":          "options.go",
			}
			wantVisits := map[string]int{}
			for provider, options := range optionsFiles {
				wantVisits[provider] = 1
				directory := filepath.Join(root, "adapters", provider)
				if err := os.MkdirAll(directory, 0700); err != nil {
					t.Fatal(err)
				}
				for name, source := range map[string]string{
					options:     "package fixture\ntype Settings struct{}\ntype Dependencies struct{}\nfunc Validate() {}\n",
					"policy.go": "package fixture\nfunc Recommend() {}\n",
				} {
					if provider == test.provider && name == test.file {
						if test.remove != "" {
							if strings.Count(source, test.remove) != 1 {
								t.Fatal("mutation must remove exactly one declaration")
							}
							source = strings.Replace(source, test.remove, "", 1)
						}
						source += test.add
						if test.misplaced {
							if err := os.WriteFile(filepath.Join(directory, "misplaced.go"), []byte("package fixture\n"+test.remove), 0600); err != nil {
								t.Fatal(err)
							}
						}
					}
					if err := os.WriteFile(filepath.Join(directory, name), []byte(source), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			const child = "TestPublicAdapterPolicyResponsibilityChild"
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+child+"$", "-test.timeout=20s", "-test.v")
			command.Env = append(os.Environ(), "FATHOMRY_POLICY_CONTROL_ROOT="+root,
				"GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
			output, err := command.CombinedOutput()
			if ctx.Err() != nil {
				t.Fatalf("policy control exceeded its deadline: %s", output)
			}
			visits, failures := map[string]int{}, map[string]int{}
			for _, line := range strings.Split(string(output), "\n") {
				if provider, ok := strings.CutPrefix(line, "=== RUN   "+child+"/"); ok {
					visits[provider]++
				}
				fields := strings.Fields(line)
				if len(fields) >= 3 && fields[0] == "---" && fields[1] == "FAIL:" {
					if provider, ok := strings.CutPrefix(fields[2], child+"/"); ok {
						failures[provider]++
					}
				}
			}
			if !maps.Equal(visits, wantVisits) {
				t.Fatalf("actual policy traversal = %v, want each fixture provider exactly once: %v\n%s", visits, wantVisits, output)
			}
			if test.diagnostic == "" {
				if err != nil || len(failures) != 0 {
					t.Fatalf("valid policy fixture failed: %v\n%s", err, output)
				}
				return
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 1 ||
				!maps.Equal(failures, map[string]int{test.provider: 1}) || !strings.Contains(string(output), test.diagnostic) {
				t.Fatalf("policy mutation did not fail at its required boundary: %v\n%s", err, output)
			}
			t.Logf("guard rejected %s: %s", test.provider, test.diagnostic)
		})
	}
}

func TestPublicAdapterPolicyResponsibilityChild(t *testing.T) {
	root := os.Getenv("FATHOMRY_POLICY_CONTROL_ROOT")
	if root == "" {
		return
	}
	profiles := map[string]adapterPackageProfile{
		"notification/lark/v1":      {role: "data-provider", catalog: true},
		"orchestration/temporal/v1": {role: "data-provider", catalog: true},
		"future/native/v1":          {role: "data-provider", catalog: true},
		"future/native/v2":          {role: "data-provider", catalog: false},
		"capability/v1":             {role: "capability"},
		"configuration/native/v1":   {role: "configuration-provider", catalog: true},
		"preparation/v1":            {role: "preparation", catalog: true},
		"v1":                        {role: "mechanism", catalog: true},
		"logging/v1":                {role: "logging-data", catalog: true},
		"ingress/v1":                {role: "ingress"},
		"composition/v1":            {role: "composition"},
		"internal/private":          {role: "private"},
	}
	checkPublicAdapterPolicyResponsibilities(t, root, profiles)
}
