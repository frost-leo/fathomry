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

package root

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	versioncmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/version"
	"github.com/frost-leo/fathomry/i18n"
)

func execute(args ...string) (int, string, string) {
	var out, diagnostic bytes.Buffer
	status := Run(context.Background(), args, strings.NewReader(""), &out, &diagnostic)
	return status, out.String(), diagnostic.String()
}

func TestCommandMatrix(t *testing.T) {
	tests := []struct {
		name, want, diagnostic string
		args                   []string
		status                 int
	}{
		{"root", "Usage:", "", nil, 0},
		{"root help", "Usage:", "", []string{"--help"}, 0},
		{"short help", "Usage:", "", []string{"-h"}, 0},
		{"help", "Usage:", "", []string{"help"}, 0},
		{"help version", "fathomry version", "", []string{"help", "version"}, 0},
		{"version help", "fathomry version", "", []string{"version", "--help"}, 0},
		{"root version", "Application module:", "", []string{"--version"}, 0},
		{"subcommand version", "Application module:", "", []string{"version"}, 0},
		{"root json", `"schema":"fathomry.cli.version/v1"`, "", []string{"--version", "--output=json"}, 0},
		{"subcommand json", `"schema":"fathomry.cli.version/v1"`, "", []string{"version", "--output", "json"}, 0},
		{"repeated output", `"schema":"fathomry.cli.version/v1"`, "", []string{"version", "--output", "text", "--output", "json"}, 0},
		{"repeated boolean false", "", "Invalid command", []string{"--version=true", "--version=false", "--output=json"}, 2},
		{"false version help", "Usage:", "", []string{"--version=false"}, 0},
		{"extra after terminator", "", "Invalid command", []string{"version", "--", "extra"}, 2},
		{"empty after terminator", "Application module:", "", []string{"version", "--"}, 0},
		{"literal help after terminator", "", "Invalid command", []string{"--", "--help"}, 2},
		{"unknown command", "", "Invalid command", []string{"missing"}, 2},
		{"unknown command help", "", "Invalid command", []string{"missing", "--help"}, 2},
		{"excluded completion help", "", "Invalid command", []string{"completion", "--help"}, 2},
		{"unknown flag", "", "Invalid command", []string{"version", "--unknown"}, 2},
		{"missing value", "", "Invalid command", []string{"version", "--output"}, 2},
		{"invalid boolean", "", "Invalid command", []string{"--version=bad"}, 2},
		{"unexpected argument", "", "Invalid command", []string{"version", "extra"}, 2},
		{"invalid output", "", "Invalid command", []string{"version", "--output=yaml"}, 2},
		{"output without request", "", "Invalid command", []string{"--output=json"}, 2},
		{"mixed root version", "", "Invalid command", []string{"--version", "version"}, 2},
		{"version root flag", "", "Invalid command", []string{"version", "--version"}, 2},
		{"no short version", "", "Invalid command", []string{"-v"}, 2},
		{"no completion", "", "Invalid command", []string{"completion"}, 2},
		{"hidden completion", "", "Invalid command", []string{"__complete", "version", ""}, 2},
		{"hidden completion alias", "", "Invalid command", []string{"__completeNoDesc", "version", ""}, 2},
		{"hidden completion help", "", "Invalid command", []string{"__complete", "--help"}, 2},
		{"hidden completion alias help", "", "Invalid command", []string{"__completeNoDesc", "--help"}, 2},
		{"help precedence", "Usage:", "", []string{"--help", "--version"}, 0},
		{"argument help precedence", "fathomry version", "", []string{"version", "extra", "--help"}, 0},
		{"semantic help precedence", "fathomry version", "", []string{"version", "--output=yaml", "--help"}, 0},
		{"syntax before help", "", "Invalid command", []string{"version", "--unknown", "--help"}, 2},
		{"unknown help target", "", "Invalid command", []string{"help", "missing"}, 2},
		{"help help", "fathomry help", "", []string{"help", "--help"}, 0},
		{"chinese help", "用法：", "", []string{"--lang=zh-CN", "--help"}, 0},
		{"chinese version", "应用模块：", "", []string{"version", "--lang", "zh-CN"}, 0},
		{"locale before command", "应用模块：", "", []string{"--lang=zh-CN", "version"}, 0},
		{"unsupported locale fallback", "Usage:", "", []string{"--lang=fr", "--help"}, 0},
		{"invalid locale", "", "Invalid command", []string{"version", "--lang=und"}, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, out, diagnostic := execute(test.args...)
			if status != test.status || !strings.Contains(out, test.want) || !strings.Contains(diagnostic, test.diagnostic) ||
				test.status == 0 && diagnostic != "" || test.status != 0 && out != "" {
				t.Fatalf("status=%d stdout=%q stderr=%q", status, out, diagnostic)
			}
		})
	}
}

func TestLocaleParityAndJSONIndependence(t *testing.T) {
	resources := append(cli.Resources(), Resources()...)
	resources = append(resources, versioncmd.Resources()...)
	catalog, err := i18n.Prepare(resources)
	if err != nil {
		t.Fatal(err)
	}
	if stale := catalog.Snapshot().Stale; len(stale) != 0 {
		t.Fatalf("stale translations: %+v", stale)
	}
	for _, locale := range []string{"en", "zh", "zh-CN", "zh-Hans", "fr", "de-DE", "und", "de-1901", "en-u-ca-gregory", "en_US", "en--US", "\x00"} {
		_, renderErr := catalog.Render(locale, "fathomry.cli.root.header", nil)
		status, _, _ := execute("version", "--output=json", "--lang="+locale)
		if (renderErr == nil) != (status == 0) {
			t.Errorf("locale %q: catalog=%v JSON status=%d", locale, renderErr, status)
		}
	}
	if status, _, _ := execute("version", "--output=json", "--lang="); status != 2 {
		t.Fatalf("explicit empty locale status=%d", status)
	}
	_, english, _ := execute("version", "--output=json", "--lang=en")
	for _, locale := range []string{"zh-CN", "fr"} {
		status, other, diagnostic := execute("version", "--output=json", "--lang="+locale)
		if status != 0 || diagnostic != "" || other != english {
			t.Fatalf("locale %q changed JSON: status=%d other=%q", locale, status, other)
		}
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(english), &record); err != nil || !strings.HasSuffix(english, "\n") {
		t.Fatalf("JSON document: %v", err)
	}
}

type cancelOnWrite struct {
	cancel context.CancelFunc
	buffer bytes.Buffer
}

func (writer *cancelOnWrite) Write(data []byte) (int, error) {
	written, err := writer.buffer.Write(data)
	writer.cancel()
	return written, err
}

func TestLateCancellationAndInputPrivacy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := &cancelOnWrite{cancel: cancel}
	if status := Run(ctx, []string{"--help"}, strings.NewReader(""), writer, io.Discard); status != 0 {
		t.Fatalf("late cancellation erased successful help: %d", status)
	}
	for _, args := range [][]string{
		{"version", "--lang=secret-canary"}, {"version", "--secret=secret-canary"},
		{"help", "secret-canary"}, {"version", "bad\x1b[31m"}, {"version", "bad\xff"},
		{strings.Repeat("x", 64<<10+1)},
	} {
		status, out, diagnostic := execute(args...)
		if status != 2 || out != "" || strings.Contains(diagnostic, "secret-canary") || len(diagnostic) > 4<<10 {
			t.Fatalf("input privacy status=%d stdout=%q stderr=%q", status, out, diagnostic)
		}
	}
}

func BenchmarkRunner(b *testing.B) {
	for _, test := range []struct {
		name string
		args []string
	}{
		{"help", []string{"--help"}},
		{"version-text", []string{"version"}},
		{"version-json", []string{"version", "--output=json"}},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if status := Run(context.Background(), test.args, strings.NewReader(""), io.Discard, io.Discard); status != 0 {
					b.Fatalf("status=%d", status)
				}
			}
		})
	}
}
