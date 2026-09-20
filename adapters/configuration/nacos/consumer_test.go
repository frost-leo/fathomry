/*
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

package nacos_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestProjectExecutable(t *testing.T) {
	f := newFixture(t, true)
	options := f.options()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "project")
	command := exec.CommandContext(ctx, filepath.Join(runtime.GOROOT(), "bin", "go"), "build", "-mod=readonly", "-o", binary, "./testdata/project")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build project: %v\n%s", err, output)
	}
	command = exec.CommandContext(ctx, binary, options.Servers[0].HTTPURL, options.Servers[0].GRPCAddress, options.Namespace, options.Sources[0].DataID)
	command.Env = append(os.Environ(), "FATHOMRY_EXAMPLE_NACOS_USERNAME="+options.Username,
		"FATHOMRY_EXAMPLE_NACOS_PASSWORD="+options.Password, "FATHOMRY_EXAMPLE_NACOS_ROOT_CA_PEM="+options.RootCAPEM,
		"FATHOMRY_EXAMPLE_NACOS_INSECURE=false")
	output, err := command.CombinedOutput()
	if err != nil || string(output) != "configuration loaded: provider=nacos schema=1\n" || f.queries.Load() != 1 || f.logins.Load() != 1 {
		t.Fatalf("standalone project failed: %v\n%s", err, output)
	}
}
