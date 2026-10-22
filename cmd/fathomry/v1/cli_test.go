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
	"strings"
	"testing"

	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func TestUnifiedCommandEntry(t *testing.T) {
	for _, args := range [][]string{{"build", "--help"}, {"new", "--help"}, {"workflow", "vet", "--help"}, {"error", "list", "--output=json"}, {"--lang", "zh-CN", "build", "--help"}, {"help", "workflow", "vet"}} {
		var official, local, officialError, localError bytes.Buffer
		first := cli.Run(context.Background(), args, cli.Options{Input: strings.NewReader(""), Output: &official, ErrorOutput: &officialError})
		handled, second := cli.Handle(context.Background(), args, cli.Project{Name: "health"}, cli.Options{Input: strings.NewReader(""), Output: &local, ErrorOutput: &localError})
		if !handled || first != nil || second != nil || !bytes.Equal(official.Bytes(), local.Bytes()) || !bytes.Equal(officialError.Bytes(), localError.Bytes()) {
			t.Fatal(args, handled, first, second)
		}
	}
	for _, args := range [][]string{nil, {"--env", "production"}, {"application-command"}} {
		if handled, err := cli.Handle(nil, args, cli.Project{}, cli.Options{}); handled || err != nil {
			t.Fatal("application startup was intercepted", args, err)
		}
	}
}

func TestProjectInspectionAndFailures(t *testing.T) {
	project := cli.Project{Name: "health", Declarations: []wf.Declaration{health.Declarations()}}
	var output, diagnostic bytes.Buffer
	options := cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}
	options.Language = "invalid_private_locale"
	handled, err := cli.Handle(context.Background(), []string{"--fathomry-project=v1"}, project, options)
	var document struct {
		Schema  string            `json:"schema"`
		Name    string            `json:"name"`
		Bundles []json.RawMessage `json:"bundles"`
	}
	if !handled || err != nil || diagnostic.Len() != 0 || json.Unmarshal(output.Bytes(), &document) != nil || document.Name != "health" || len(document.Bundles) != 1 {
		t.Fatal(handled, err, output.String(), diagnostic.String())
	}
	options.Language = "en"
	for _, args := range [][]string{{"--fathomry-project=v2"}, {"--fathomry-project=v1", "private-canary"}, {"build", "--unknown=private-canary"}, {"--output=json", "build", "--unknown=private-canary"}} {
		output.Reset()
		diagnostic.Reset()
		handled, err := cli.Handle(context.Background(), args, project, options)
		if !handled || err == nil || cli.ExitCode(err) != 2 || output.Len() != 0 || diagnostic.Len() == 0 || strings.Contains(diagnostic.String(), "private-canary") {
			t.Fatal(args, handled, err, diagnostic.String())
		}
	}
}
