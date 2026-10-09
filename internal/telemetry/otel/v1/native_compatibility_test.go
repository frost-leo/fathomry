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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNativeCompatibilityUsesConsumingSelectionsOffline(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("test source absent")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "../../../.."))
	goBinary := filepath.Join(runtime.GOROOT(), "bin", "go")
	environment := append(os.Environ(), "GOTOOLCHAIN=local", "GOWORK=off", "GOPROXY=off", "GOSUMDB=off", "GOFLAGS=")
	for _, path := range []string{"go.opentelemetry.io/otel/sdk/metric/internal/aggregate", "go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/tracetransform"} {
		t.Run(path, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			configuration := exec.CommandContext(ctx, goBinary, "env", "CGO_ENABLED")
			configuration.Dir, configuration.Env = root, environment
			cgo, err := configuration.Output()
			if err != nil {
				t.Fatal(err)
			}
			arguments := []string{"test", "-mod=readonly", "-p=2", "-count=1", "-timeout=45s", "-run=^(TestFathomry|FuzzFathomry)", "-v"}
			if strings.TrimSpace(string(cgo)) == "1" {
				arguments = append(arguments, "-race")
			}
			arguments = append(arguments, path)
			command := exec.CommandContext(ctx, goBinary, arguments...)
			command.Dir, command.Env = root, environment
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("native controls on consuming graph failed: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "--- PASS: TestFathomry") {
				t.Fatal("native subtest executed no compatibility controls", path)
			}
		})
	}
}
