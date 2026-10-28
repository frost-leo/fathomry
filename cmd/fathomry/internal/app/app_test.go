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

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
)

func TestConcurrentInvocations(t *testing.T) {
	var wait sync.WaitGroup
	for range 16 {
		wait.Go(func() {
			var output, diagnostic bytes.Buffer
			err := Run(context.Background(), []string{"--help"}, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
			if err != nil || output.Len() == 0 || diagnostic.Len() != 0 {
				t.Error(err, output.String(), diagnostic.String())
			}
		})
	}
	wait.Wait()
}

func TestProjectCommandBoundaries(t *testing.T) {
	parent := t.TempDir()
	t.Chdir(parent)
	for _, sample := range []struct {
		args []string
		code int
	}{
		{[]string{"project", "--help"}, 0},
		{[]string{"--lang=zh-CN", "project", "new", "--help"}, 0},
		{[]string{"project", "check", "--help"}, 2},
		{[]string{"project", "init"}, 2},
		{[]string{"project", "create"}, 2},
		{[]string{"project", "new"}, 2},
		{[]string{"project", "new", "--name=demo", "--module=example.com/demo", "--framework-version=latest"}, 2},
	} {
		var output, diagnostic bytes.Buffer
		err := Run(context.Background(), sample.args, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		if command.ExitCode(err) != sample.code {
			t.Fatal(sample.args, err, output.String())
		}
		entries, err := os.ReadDir(parent)
		if err != nil || len(entries) != 0 {
			t.Fatal("non-creation invocation changed filesystem", err)
		}
	}
	var output, diagnostic bytes.Buffer
	err := Run(context.Background(), []string{"--output=json", "project", "new", "--name=demo", "--module=example.com/demo", "--framework-version=v0.0.0-20261028081050-b28e37d55e53"}, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
	var result struct {
		Data struct {
			State       string
			Directories []string
			Files       []json.RawMessage
		}
	}
	if err != nil || diagnostic.Len() != 0 || json.Unmarshal(output.Bytes(), &result) != nil || result.Data.State != "complete" || len(result.Data.Directories) != 4 || len(result.Data.Files) != 6 {
		t.Fatal("actual command failed", err, output.String(), diagnostic.String())
	}
}
