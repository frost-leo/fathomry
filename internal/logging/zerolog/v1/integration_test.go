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
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/compatibility"
	"github.com/frost-leo/fathomry/internal/conformance"
)

func TestFacadePrivacyProfileAndMissingEvidence(t *testing.T) {
	options := fileOptions(t, true, 4)
	f := bindFixture(t, options, 1)
	profile := f.logger.Profile()
	profile.Options[0].Value = "changed"
	if f.logger.Profile().Options[0].Value != "info" {
		t.Fatal("profile aliases caller")
	}
	build, err := compatibility.Inspect(compatibility.BuildRequest{SDKModules: []string{"github.com/rs/zerolog"}})
	if err != nil {
		t.Fatal(err)
	}
	report, err := compatibility.Assess(build, f.logger.access, f.logger.Profile(),
		[]compatibility.Requirement{{Guarantee: "bounded-logging", Layers: []compatibility.Layer{compatibility.Capability, compatibility.SDK}}}, nil)
	if err != nil || report.Source.Configuration.Revision != f.logger.access.Info().Configuration.Revision || report.Require(compatibility.Policy{}) == nil {
		t.Fatal("missing evidence became certified support", err)
	}
	conformance.Facade(t, f.logger, "Log", "Rotate", "Sync", "Profile", "Format", "LogValue", "MarshalJSON", "UnmarshalJSON")
	conformance.Runtime(t, f.logger, new(Logger), options.Sinks[0].File.Directory)
	conformance.Runtime(t, *f.logger, new(Logger), options.Sinks[0].File.Directory)
	conformance.Runtime(t, options.Sinks[0], new(SinkV1), options.Sinks[0].File.Directory)
	conformance.Runtime(t, *options.Sinks[0].File, new(FileOptionsV1), options.Sinks[0].File.Directory)
}
func TestActualConsumingExecutableAndBinaryEncodingRefusal(t *testing.T) {
	for _, binaryEncoding := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "binary-refusal"}[binaryEncoding], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			binary := filepath.Join(t.TempDir(), "consumer")
			args := []string{"build", "-o", binary}
			if binaryEncoding {
				args = append(args, "-tags=binary_log")
			}
			args = append(args, "./testdata/consumer")
			command := exec.CommandContext(ctx, "go", args...)
			command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("consumer build failed: %v\n%s", err, output)
			}
			output, err := exec.CommandContext(ctx, binary).Output()
			if err != nil {
				t.Fatal("consumer execution failed")
			}
			var result struct {
				Go       string
				Executed bool
				Refused  bool
				Records  int
				Modules  map[string]string
			}
			if err := json.Unmarshal(output, &result); err != nil {
				t.Fatal("invalid consumer summary")
			}
			if result.Go != runtime.Version() || result.Refused != binaryEncoding || result.Executed == binaryEncoding ||
				!binaryEncoding && result.Records != 2 {
				t.Fatal("consumer execution scope changed")
			}
			info, err := buildinfo.ReadFile(binary)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, module := range info.Deps {
				if module.Path == "github.com/rs/zerolog" {
					found = module.Version == "v1.35.1" && module.Replace == nil && module.Sum != "" && result.Modules[module.Path] == module.Version
				}
				if module.Path == "go.uber.org/zap" || strings.Contains(module.Path, "lumberjack") || strings.Contains(module.Path, "timberjack") {
					t.Fatal("zerolog consumer imported another logging/rotation engine")
				}
			}
			if !found {
				t.Fatal("consumer uses unverified zerolog dependency")
			}
		})
	}
}
func TestLoggingDependencyDirectionAndExternalImportRejection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	run := func(dir string, args ...string) ([]byte, error) {
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = dir
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		return command.CombinedOutput()
	}
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	output, err := run(root, "list", "-deps", "./internal/logging/zerolog/v1")
	if err != nil {
		t.Fatal("dependency inspection failed")
	}
	for _, path := range strings.Fields(string(output)) {
		if path == "testing" || strings.HasSuffix(path, "/internal/conformance") || path == "go.uber.org/zap" ||
			strings.Contains(path, "/internal/database/") || strings.Contains(path, "/internal/configsource/") {
			t.Fatal("logging imported unrelated runtime or test mechanism")
		}
	}
	directory := t.TempDir()
	module := "module example.org/logging-consumer\n\ngo 1.26.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => " + strconv.Quote(filepath.ToSlash(root)) + "\n"
	if err := os.WriteFile(filepath.Join(directory, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "consumer.go"), []byte("package consumer\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := run(directory, "test", "-mod=mod", "./..."); err != nil {
		t.Fatalf("external positive control failed: %v\n%s", err, output)
	}
	path := "github.com/frost-leo/fathomry/internal/logging/zerolog/v1"
	if err := os.WriteFile(filepath.Join(directory, "consumer.go"), []byte("package consumer\nimport _ "+strconv.Quote(path)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := run(directory, "test", "-mod=mod", "./..."); err == nil || !strings.Contains(string(output), "use of internal package "+path+" not allowed") {
		t.Fatalf("wrong private import rejection: %v\n%s", err, output)
	}
}

type panickingSink struct{}

func (panickingSink) Write([]byte) (int, error) { panic("panic-canary") }
func TestSinkPanicRetainsUnknownEffectAndDoesNotStrandOwnership(t *testing.T) {
	f := bindFixture(t, OptionsV1{Name: "panic", Sinks: []SinkV1{{Name: "bad", Writer: panickingSink{}}, {Name: "good", Writer: io.Discard}}}, 1)
	result := emit(t, f, "panic", Info, "message")
	sinks := result.Outcome.Value.SinksCopy()
	if !errors.Is(result.Err(), ErrWrite) || !sinks[0].Attempted || sinks[0].Accepted || sinks[0].BytesKnown || !sinks[1].Accepted {
		t.Fatal("panic boundary invented success or lost later sink")
	}
	conformance.Private(t, result.Err(), "panic-canary")
	drain(t, f.inbox)
}
