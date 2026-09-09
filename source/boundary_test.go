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

package source_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndependentModuleUsesPublicFoundation(t *testing.T) {
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	quoted, _ := json.Marshal(filepath.ToSlash(root))
	module := "module example.org/consumer\n\ngo 1.26.0\n\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => " + string(quoted) + "\n"
	program := `package consumer
import (
	"context"
	"errors"
	"testing"
	"time"
	"github.com/frost-leo/fathomry/compatibility"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/operation"
	"github.com/frost-leo/fathomry/source"
)
type config struct{}
func TestPublicComposition(t *testing.T) {
	identity := failure.MustDefine(failure.Definition{Code:"consumer.example.failed", Component:"consumer", Version:1})
	if !errors.Is(identity.New(failure.Attribution{}, context.Canceled), context.Canceled) { t.Fatal("cause inspection") }
	settings, err := source.Prepare(source.Schema[config]{Format:1}, source.Input{
		Identity:source.Identity{Provider:"example.local", Name:"one"}, Format:1,
	})
	if err != nil { t.Fatal(err) }
	selected := source.WithLimits(source.Select(settings, func(context.Context, config) (source.Resource[func() string], error) {
		return source.Resource[func() string]{Acquired:true, Capability:func() string { return "one" },
			Release:func(context.Context) source.ReleaseResult { return source.ReleaseResult{Released:true, Quiescent:true} },
		}, nil
	}), source.Limits{Active:1, Bytes:128, MaxLeases:2})
	assembly, err := source.Assemble(context.Background(), context.Background(), "consumer", selected)
	if err != nil { t.Fatal(err) }
	value, _, err := source.Bind(assembly, selected)
	if err != nil || value() != "one" { t.Fatal("public binding") }
	access, err := source.AccessFor(assembly, selected)
	if err != nil { t.Fatal(err) }
	inbox, err := operation.NewInbox[string](1, 128)
	if err != nil { t.Fatal(err) }
	ctx := context.Background()
	call, err := operation.Begin(ctx, access, operation.Request{Name:"read", Shape:operation.Finite,
		Execution:failure.Execution{Call:"external-call"}, Bytes:128, EvidenceBytes:128,
		Admission:operation.Budget{Limit:1000000000}}, inbox, nil)
	if err != nil { t.Fatal(err) }
	if err := call.Execute(ctx, operation.Budget{Limit:1000000000}, func(context.Context, operation.Scope) operation.Outcome[string] {
		return operation.Outcome[string]{Value:value(), Present:true}
	}); err != nil { t.Fatal(err) }
	result, err := call.Receipt().WaitReleased(ctx)
	if err != nil || !result.Final || result.Outcome.Value != "one" { t.Fatal("public result") }
	wait, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	delivery, err := inbox.Next(wait)
	if err != nil { t.Fatal(err) }
	evidence, err := delivery.Receipt().WaitReleased(wait)
	if err != nil || !evidence.Final || !evidence.Released || evidence.Outcome.Value != "one" || evidence.Attribution.Execution.Call != "external-call" {
		t.Fatal("wrong public evidence")
	}
	if err := delivery.Release(); err != nil { t.Fatal(err) }
	build, err := compatibility.Inspect(compatibility.BuildRequest{
		SDKModules:[]string{"go.yaml.in/yaml/v3"}, DisclosePaths:[]string{"example.org/consumer"},
	})
	if err != nil { t.Fatal(err) }
	absent := compatibility.Fact{Kind:compatibility.NotApplicable}
	profile := compatibility.Profile{ImplementationModule:"example.org/consumer", SDKMode:"local",
		ServiceMode:absent, ServiceVersion:absent, Protocol:absent, Native:absent}
	diagnostic, err := compatibility.Assess(build, access, profile,
		[]compatibility.Requirement{{Guarantee:"public-call-provenance", Layers:[]compatibility.Layer{compatibility.Mechanism}}}, nil)
	if err != nil || diagnostic.Source.Scope != "consumer" || diagnostic.Actual.SourceRevision != access.Info().Configuration.Revision ||
		!errors.Is(diagnostic.Require(compatibility.Policy{}), compatibility.ErrUnverified) {
		t.Fatal("public composition diagnostics lost source facts or accepted missing evidence")
	}
	if err := assembly.Close(context.Background()); err != nil { t.Fatal(err) }
}
`
	header, err := os.ReadFile(filepath.Join(root, ".github", "LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	notice := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(header)), "\n", "\n * ") + "\n */\n"
	program = notice + program
	for name, content := range map[string]string{"go.mod": module, "consumer_test.go": program} {
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("external consumer: %v\n%s", err, output)
	}
	internal := "github.com/frost-leo/fathomry/internal/conformance"
	if err := os.WriteFile(filepath.Join(directory, "consumer_test.go"), []byte(notice+"package consumer\nimport _ \""+internal+"\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	command = exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOTOOLCHAIN=local")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "use of internal package "+internal+" not allowed") {
		t.Fatal("external consumer did not encounter the intended internal boundary")
	}

	command = exec.Command("go", "list", "-deps", "github.com/frost-leo/fathomry/failure")
	output, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.Contains(strings.Split(dependency, "/")[0], ".") && dependency != "github.com/frost-leo/fathomry/failure" {
			t.Fatalf("error foundation depends on consumer or Provider: %s", dependency)
		}
	}
	command = exec.Command("go", "list", "-deps", "github.com/frost-leo/fathomry/source", "github.com/frost-leo/fathomry/operation", "github.com/frost-leo/fathomry/compatibility")
	output, err = command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if dependency == internal || dependency == "testing" {
			t.Fatal("production foundation imports internal testing support")
		}
	}
}
