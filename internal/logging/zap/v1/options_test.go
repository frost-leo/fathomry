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

package zap

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func newResultInbox() (*invocation.Inbox[Result], error) {
	return invocation.NewInbox[Result](4, 4*(16<<10))
}
func resourceAssembly(selected resource.Selection[Source]) (*resource.Assembly, error) {
	return resource.Assemble(context.Background(), context.Background(), "options", selected)
}
func TestPreparationPreflightsInvalidOptionsAndLayers(t *testing.T) {
	sink := &recordingSink{}
	options := fileOptions(t, false)
	for _, patch := range []func(*OptionsV1){
		func(value *OptionsV1) { value.Outputs = make([]OutputV1, MaxSinks+1) },
		func(value *OptionsV1) { value.ExtensionLevel = strings.Repeat("a", 17) },
		func(value *OptionsV1) { value.Outputs[0].Directory = strings.Repeat("a", 4097) },
		func(value *OptionsV1) { value.Outputs[0].Name = strings.Repeat("a", 65) },
		func(value *OptionsV1) { value.Name = "" }, func(value *OptionsV1) { value.QueuedCalls = -1 },
		func(value *OptionsV1) { value.Timeout = -time.Second }, func(value *OptionsV1) { value.MaxEntryBytes = 1023 },
		func(value *OptionsV1) { value.Outputs[0].Directory = "relative" },
		func(value *OptionsV1) { value.Outputs[0].MaxBackups = -1 },
		func(value *OptionsV1) { value.Outputs[0].MaxFileBytes = 1 << 30 },
		func(value *OptionsV1) { value.Outputs[0].Level = "fatal" },
		func(value *OptionsV1) { value.Outputs = append(value.Outputs, value.Outputs[0]) },
		func(value *OptionsV1) { value.Outputs[0].Kind = "network" },
		func(value *OptionsV1) { value.Outputs[0].Kind = "stdout" },
	} {
		value := options
		value.Outputs = append([]OutputV1(nil), options.Outputs...)
		patch(&value)
		if _, err := Select(value, sink); err == nil {
			t.Fatal("invalid settings accepted")
		}
	}
	for _, content := range []string{"unknown: true", "timeout_ns: null", "max_entry_bytes: 0", "outputs: null", "outputs: [{name: file, kind: file}]", "queued_calls: nope", "timeout_ns: !!int 1000000", "caller: true\ncaller: false"} {
		extension := StructuredSink(sink)
		if content == "outputs: null" {
			extension = nil
		}
		if _, err := Select(options, extension, resource.Layer{Kind: resource.Base, Content: []byte(content)}); err == nil {
			t.Fatal("invalid overlay accepted")
		}
	}
	if _, err := Select(options, sink, resource.Layer{Kind: resource.Base, Content: []byte("timeout_ns: invalid")},
		resource.Layer{Kind: resource.Local, Content: []byte("timeout_ns: 1000000")}); err == nil {
		t.Fatal("higher layer hid invalid lower input")
	}
	if entries, err := os.ReadDir(options.Outputs[0].Directory); err != nil || len(entries) != 0 {
		t.Fatal("preparation opened output before validation", err)
	}
	var absent *recordingSink
	if _, err := Select(OptionsV1{Name: "logs"}, absent); err == nil {
		t.Fatal("typed nil extension accepted")
	}
}

func TestEffectiveProfileAndPreparationAreIsolated(t *testing.T) {
	options := fileOptions(t, true)
	directory := options.Outputs[0].Directory
	selected, err := Select(options, nil, resource.Layer{Kind: resource.Base, Content: []byte("queued_calls: 2\ncaller: true")})
	if err != nil {
		t.Fatal(err)
	}
	options.Outputs[0].Directory = filepath.Join(t.TempDir(), "private-path-canary")
	options.QueuedCalls = 2
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resourceAssembly(selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, err := newResultInbox()
	if err != nil {
		t.Fatal(err)
	}
	logger, err := Bind(assembly, selected, inbox, nil)
	if err != nil {
		t.Fatal(err)
	}
	if logger.owner.settings.Outputs[0].Directory != directory || !logger.owner.settings.Caller || logger.owner.settings.QueuedCalls != 2 {
		t.Fatal("effective preparation aliased bootstrap")
	}
	profile := logger.Profile()
	profile.Options[0].Value = "changed"
	if logger.Profile().Options[0].Value != "json" {
		t.Fatal("profile aliases owner")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"go.uber.org/zap", "go.uber.org/multierr", "golang.org/x/sys"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, logger.access, logger.Profile(), []compatibility.Requirement{{Guarantee: "logging", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK}}}, nil)
	if err != nil || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("missing evidence certified support", err)
	}
	conformance.Private(t, logger, "private-path-canary", directory)
	for _, option := range logger.Profile().Options {
		if strings.Contains(option.Value, directory) {
			t.Fatal("profile disclosed path")
		}
	}
}

func TestLayerValidationSeparatesStructureFromEffectiveSemantics(t *testing.T) {
	for _, test := range []struct {
		name     string
		base     string
		rejected bool
	}{
		{"overridden-semantic-error", "timeout_ns: -1", false},
		{"overridden-type-error", "timeout_ns: invalid", true},
		{"unknown-field", "unknown_timeout_ns: 1000000", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			selected, err := Select(OptionsV1{Name: "layers"}, &recordingSink{},
				resource.Layer{Kind: resource.Base, Content: []byte(test.base)},
				resource.Layer{Kind: resource.Local, Content: []byte("timeout_ns: 1000000")})
			if test.rejected {
				if !errors.Is(err, resource.ErrConfiguration) {
					t.Fatal("structurally invalid lower layer was not rejected", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			assembly, err := resourceAssembly(selected)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := assembly.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			source, _, err := resource.Bind(assembly, selected)
			if err != nil {
				t.Fatal(err)
			}
			if source.owner.settings.Timeout != time.Millisecond {
				t.Fatal("semantic validation did not use the final effective value")
			}
		})
	}
}

func TestBindRejectsWidenedSerialPolicy(t *testing.T) {
	options := OptionsV1{Name: "logs"}
	selected, err := Select(options, &recordingSink{})
	if err != nil {
		t.Fatal(err)
	}
	limits := LimitsV1(options)
	limits.Active = 2
	selected = resource.WithLimits(selected, limits)
	assembly, err := resourceAssembly(selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	inbox, _ := newResultInbox()
	if _, err := Bind(assembly, selected, inbox, nil); !errors.Is(err, ErrInput) {
		t.Fatal("parallel authority bypassed source serialization")
	}
}

func FuzzOptionsPreparation(f *testing.F) {
	for _, seed := range []string{"{}", "queued_calls: 3", "timeout_ns: 1000000", "outputs: null", "a: b"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, document []byte) {
		if len(document) > 8192 {
			return
		}
		_, _ = Select(OptionsV1{Name: "fuzz"}, &recordingSink{}, resource.Layer{Kind: resource.Base, Content: document})
	})
}
