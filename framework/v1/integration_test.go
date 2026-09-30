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

package framework_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestIndependentConsumer(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	source, err := os.ReadFile("testdata/consumer/consumer_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string][]byte{
		"consumer_test.go": source,
		"go.mod":           []byte(fmt.Sprintf("module example.org/framework-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(offline bool, args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local")
		if offline {
			command.Env = append(command.Env, "GOPROXY=off", "GOSUMDB=off")
		}
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent command exceeded its bound")
		}
		return output, err
	}
	// Prepare the consumer's complete graph before enforcing offline checks.
	if output, err := run(false, "mod", "tidy"); err != nil {
		t.Fatalf("consumer dependency preparation: %v\n%s", err, output)
	}
	for _, args := range [][]string{{"mod", "tidy", "-diff"}, {"test", "-mod=readonly", "-race", "-count=1", "./..."}} {
		if output, err := run(true, args...); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
	}
	output, err := run(true, "list", "-deps", "github.com/frost-leo/fathomry/framework/v1")
	if err != nil || strings.Contains(string(output), "github.com/frost-leo/fathomry/internal/") {
		t.Fatal("Framework common imported Internal", err)
	}
}
