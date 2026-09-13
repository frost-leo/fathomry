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

package otel_test

import (
	"context"
	"debug/buildinfo"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestActualConsumingBinaryBuildFactsAndProviderIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "otel-consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer behavior failed")
	}
	var report struct {
		Go                         string
		Modules                    map[string]string
		Queued, Received, Released bool
	}
	if json.Unmarshal(output, &report) != nil || report.Go != runtime.Version() || !report.Queued || !report.Received || !report.Released {
		t.Fatal("consumer observations failed")
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		"go.opentelemetry.io/otel": "v1.46.0", "go.opentelemetry.io/otel/sdk": "v1.46.0", "go.opentelemetry.io/otel/trace": "v1.46.0",
		"go.opentelemetry.io/otel/metric": "v1.46.0", "go.opentelemetry.io/otel/log": "v0.22.0", "go.opentelemetry.io/otel/sdk/log": "v0.22.0",
		"go.opentelemetry.io/otel/sdk/metric": "v1.46.0", "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp": "v0.22.0",
		"go.opentelemetry.io/otel/exporters/otlp/otlptrace": "v1.46.0", "go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp": "v1.46.0",
		"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp": "v1.46.0", "go.opentelemetry.io/proto/otlp": "v1.11.0",
	}
	for path, version := range expected {
		found := false
		for _, module := range build.Deps {
			if module.Path == path {
				found = module.Version == version && module.Replace == nil && module.Sum != "" && report.Modules[path] == version
			}
		}
		if !found {
			t.Fatalf("unverified contributing module %s", path)
		}
	}
	for _, module := range build.Deps {
		for _, unselected := range []string{"zerolog", "go.uber.org/zap", "nacos", "pgx", "go-sql-driver", "spf13/viper"} {
			if strings.Contains(module.Path, unselected) {
				t.Fatal("core telemetry linked an unselected provider")
			}
		}
	}
	command = exec.CommandContext(ctx, "go", "list", "-deps", "./")
	command.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off")
	dependencies, err := command.Output()
	if err != nil {
		t.Fatal("dependency inspection failed")
	}
	if strings.Contains(string(dependencies), "github.com/frost-leo/fathomry/internal/conformance\n") || strings.Contains(string(dependencies), "\ntesting\n") {
		t.Fatal("test support became a production dependency")
	}
}
