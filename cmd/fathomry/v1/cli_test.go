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

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
)

func TestMinimalCLI(t *testing.T) {
	for _, args := range [][]string{nil, {"--help"}, {"help"}, {"--lang", "zh-CN", "--help"}, {"--output=json", "help"}} {
		var output, diagnostic bytes.Buffer
		err := cli.Run(context.Background(), args, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		if err != nil || output.Len() == 0 || diagnostic.Len() != 0 {
			t.Fatal(args, err, output.String(), diagnostic.String())
		}
		if len(args) > 0 && args[0] == "--output=json" && !json.Valid(output.Bytes()) {
			t.Fatal("invalid help JSON")
		}
	}
}

func TestWithdrawnCommandsReject(t *testing.T) {
	for _, name := range []string{"new", "build", "project", "workflow", "mode", "namespace", "schema", "database", "error", "i18n", "messages", "--fathomry-project=v1", "--fathomry-project=v2"} {
		t.Run(name, func(t *testing.T) {
			for _, suffix := range [][]string{nil, {"--help"}} {
				var output, diagnostic bytes.Buffer
				args := append([]string{"--output=json", name}, suffix...)
				err := cli.Run(context.Background(), args, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
				if cli.ExitCode(err) != 2 || output.Len() != 0 || !json.Valid(diagnostic.Bytes()) {
					t.Fatal("withdrawn entry accepted or lost diagnostic", err, output.String(), diagnostic.String())
				}
			}
		})
	}
}

func TestCancellationAndInvalidOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var output, diagnostic bytes.Buffer
	options := cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}
	if err := cli.Run(ctx, []string{"help"}, options); !errors.Is(err, context.Canceled) || cli.ExitCode(err) != 130 {
		t.Fatal("lost cancellation", err)
	}
	if err := cli.Run(context.Background(), nil, cli.Options{}); err == nil {
		t.Fatal("missing streams accepted")
	}
}
