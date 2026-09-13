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

package zerolog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsFrozenLayersAndInvalidPreflight(t *testing.T) {
	var writer bytes.Buffer
	file := &FileOptionsV1{Directory: privateDir(t)}
	options := OptionsV1{Name: "options", Sinks: []SinkV1{{Name: "writer", Writer: &writer}, {Name: "file", File: file}}}
	layer := []byte("min_level: warn\ntimeout_ns: 1000000000")
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: layer})
	if err != nil {
		t.Fatal(err)
	}
	file.Directory = "changed-canary"
	options.Sinks[0].Name = "changed"
	layer[0] = 'X'
	assembly, err := resource.Assemble(context.Background(), context.Background(), "options", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	source, info, err := resource.Bind(assembly, selected)
	if err != nil || source.owner.settings.Sinks[0].Name != "writer" || source.owner.settings.MinLevel != "warn" || source.owner.settings.Timeout != time.Second ||
		source.owner.settings.Sinks[1].File.Directory == "changed-canary" || info.Configuration.Format != 1 || info.Configuration.Revision == "" {
		t.Fatal("configuration/default/overlay aliasing")
	}
	conformance.Runtime(t, options, new(OptionsV1), "changed-canary")
	for _, raw := range []string{"unknown: secret", "timeout_ns: 0", "max_record_bytes: 0", "queued_calls: 65", "min_level: nonsense",
		"sinks: []", "sinks: null", "sinks: [{name: renamed, kind: writer, min_level: info}]", "sinks: [{name: writer, kind: record, min_level: info}]"} {
		if _, err := Select(OptionsV1{Name: "invalid", Sinks: []SinkV1{{Name: "writer", Writer: io.Discard}}}, resource.Layer{Kind: resource.Base, Content: []byte(raw)}); err == nil {
			t.Fatal("invalid layer accepted")
		}
	}
	// Structural invalidity in a lower layer remains invalid even if overridden.
	if _, err := Select(OptionsV1{Name: "invalid", Sinks: []SinkV1{{Name: "writer", Writer: io.Discard}}},
		resource.Layer{Kind: resource.Base, Content: []byte("timeout_ns: wrong")},
		resource.Layer{Kind: resource.Local, Content: []byte("timeout_ns: 1000000")}); err == nil {
		t.Fatal("invalid overridden field hidden")
	}
}
func TestInvalidSinksAndLimitsRejectBeforeUse(t *testing.T) {
	var nilWriter *bytes.Buffer
	directory := t.TempDir()
	for _, sinks := range [][]SinkV1{
		nil, {{Name: "none"}}, {{Name: "nil", Writer: nilWriter}}, {{Name: "ambiguous", Writer: io.Discard, File: &FileOptionsV1{Directory: directory}}},
		{{Name: "repeat", Writer: io.Discard}, {Name: "repeat", Writer: io.Discard}},
		{{Name: "one", File: &FileOptionsV1{Directory: directory}}, {Name: "two", File: &FileOptionsV1{Directory: directory}}},
		{{Name: "relative", File: &FileOptionsV1{Directory: "relative"}}},
		{{Name: "small", File: &FileOptionsV1{Directory: directory, MaxBytes: 1024}}},
		{{Name: "unbounded", File: &FileOptionsV1{Directory: directory, Backups: 129}}},
	} {
		if _, err := Select(OptionsV1{Name: "invalid", Sinks: sinks}); err == nil {
			t.Fatal("invalid sinks admitted")
		}
	}
	options := OptionsV1{Name: "limits", Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	if assembly, err := resource.Assemble(context.Background(), context.Background(), "duplicate", selected, selected); assembly != nil || !errors.Is(err, resource.ErrSelection) {
		t.Fatal("duplicate source reached construction")
	}
	limits := LimitsV1(options)
	limits.Active = 2
	selected = resource.WithLimits(selected, limits)
	assembly, err := resource.Assemble(context.Background(), context.Background(), "limits", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer assembly.Close(context.Background())
	if logger, err := Bind(assembly, selected, nil, nil); logger != nil || err == nil {
		t.Fatal("invalid binding accepted")
	}
	inbox := bindFixture(t, options, 1).inbox
	if logger, err := Bind(assembly, selected, inbox, nil); logger != nil || !errors.Is(err, ErrInput) {
		t.Fatal("multi-active writer bypass allowed")
	}
}
func TestZeroValuesAndUnsupportedMaintenance(t *testing.T) {
	var logger *Logger
	if receipt, err := logger.Log(context.Background(), correlation("zero"), Info, "message"); receipt != nil || err == nil {
		t.Fatal("nil logger accepted")
	}
	if logger.Profile().SDKMode != "" || (Record{}).JSONCopy() != nil || (Result{}).SinksCopy() != nil {
		t.Fatal("zero state invented facts")
	}
	conformance.Private(t, logger, "never-present")
	f := bindFixture(t, OptionsV1{Name: "writer", Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}}, 1)
	if receipt, err := f.logger.Sync(context.Background(), correlation("sync")); receipt != nil || !errors.Is(err, ErrUnsupported) {
		t.Fatal("borrowed flush claimed")
	}
	if receipt, err := f.logger.Log(context.Background(), correlation("invalid"), Level("unknown"), ""); receipt != nil || !errors.Is(err, ErrInput) {
		t.Fatal("unknown severity accepted")
	}
}

func TestBootstrapSizeRefusalPrecedesSerialization(t *testing.T) {
	for _, target := range []string{"source-level", "sink-level", "sink-name", "aggregate"} {
		t.Run(target, func(t *testing.T) {
			options := OptionsV1{Name: "bounded", Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}}
			oversize := strings.Repeat("x", 8<<20)
			switch target {
			case "source-level":
				options.MinLevel = Level(oversize)
			case "sink-level":
				options.Sinks[0].MinLevel = Level(oversize)
			case "sink-name":
				options.Sinks[0].Name = oversize
			case "aggregate":
				options.MinLevel = Level(oversize[:600<<10])
				options.Sinks[0].MinLevel = Level(oversize[:600<<10])
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := Select(options)
			runtime.ReadMemStats(&after)
			if !errors.Is(err, resource.ErrConfiguration) {
				t.Fatal("configuration identity lost", err)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("oversized bootstrap allocated %d bytes before rejection", allocated)
			if allocated > 1<<20 {
				t.Fatal("oversized bootstrap was copied/serialized before rejection", allocated)
			}
		})
	}
}

func TestBootstrapByteGuardPreservesEffectiveOverlayValidation(t *testing.T) {
	options := OptionsV1{Name: "overlay", MinLevel: Level(strings.Repeat("x", 65)), Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}}
	if _, err := Select(options); !errors.Is(err, resource.ErrConfiguration) {
		t.Fatal("invalid effective default accepted")
	}
	f := bindFixture(t, options, 1, resource.Layer{Kind: resource.Local, Content: []byte("min_level: info")})
	if f.logger.owner.settings.MinLevel != "info" {
		t.Fatal("repairable default rejected or ignored")
	}
	if result := emit(t, f, "overridden", Info, "message"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
}

func FuzzOptionsLayers(f *testing.F) {
	f.Add("min_level: debug")
	f.Add("max_record_bytes: 1024\nqueued_calls: 0")
	f.Add("sinks: null")
	f.Fuzz(func(t *testing.T, layer string) {
		if len(layer) > 8192 {
			return
		}
		_, err := Select(OptionsV1{Name: "fuzz", Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}},
			resource.Layer{Kind: resource.Local, Content: []byte(layer)})
		if err != nil {
			conformance.Private(t, err, "credential-canary-never-render")
		}
	})
}
func TestRecordLimitIncludesEscapingButDoesNotTruncate(t *testing.T) {
	var output bytes.Buffer
	f := bindFixture(t, OptionsV1{Name: "bounds", MaxRecordBytes: 1024, Sinks: []SinkV1{{Name: "out", Writer: &output}}}, 1)
	if receipt, err := f.logger.Log(context.Background(), correlation("large"), Info, strings.Repeat("x", 1025)); receipt != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("oversize input admitted")
	}
	if f.inbox.Usage().Outstanding != 0 || output.Len() != 0 {
		t.Fatal("oversize input had effects")
	}
}
