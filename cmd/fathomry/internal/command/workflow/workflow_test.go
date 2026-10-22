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

package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

func TestWorkflowCommands(t *testing.T) {
	components := append(i18n.CoreComponents(), command.Component())
	components = append(components, Components()...)
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	atlas, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	coverage, err := messages.Coverage("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range coverage {
		if len(owner.Missing) != 0 {
			t.Fatal("group or leaf translation is missing", owner.Component)
		}
	}
	run := func(args []string, locale string) (string, string, error) {
		t.Helper()
		var output, diagnostic bytes.Buffer
		err := command.Run(context.Background(), append(append([]string(nil), args...), "--output", "json"),
			command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic, Language: locale},
			command.Catalogs{Errors: atlas, Messages: messages}, func(invocation *command.Invocation) *cobra.Command {
				root := invocation.Group("fathomry", "fathomry.command_line.root")
				root.AddCommand(New(invocation))
				return root
			})
		return output.String(), diagnostic.String(), err
	}
	for _, locale := range []string{"en", "zh-CN", "fr"} {
		for _, test := range []struct {
			args  []string
			group bool
		}{
			{[]string{"workflow"}, true},
			{[]string{"workflow", "--help"}, true},
			{[]string{"workflow", "vet", "--help"}, false},
			{[]string{"help", "workflow", "vet"}, false},
		} {
			output, diagnostic, err := run(test.args, locale)
			if err != nil || diagnostic != "" {
				t.Fatal(locale, test.args, err, diagnostic)
			}
			var envelope struct {
				Schema, Command string
				Data            struct {
					Usage, Description string
					Commands           []struct{ Name, Description string }
				}
			}
			if err := json.Unmarshal([]byte(output), &envelope); err != nil || envelope.Schema != command.Schema || envelope.Command != "help" {
				t.Fatal("invalid help envelope", err, output)
			}
			usage := "fathomry workflow vet BUNDLE [BUNDLE...]"
			message := "fathomry.workflow_vet.command"
			if test.group {
				usage = "fathomry workflow"
				message = "fathomry.workflow_commands.root"
				if len(envelope.Data.Commands) != 1 || envelope.Data.Commands[0].Name != "vet" || envelope.Data.Commands[0].Description == "" {
					t.Fatal("group does not expose exactly the implemented leaf", output)
				}
			} else if len(envelope.Data.Commands) != 0 {
				t.Fatal("leaf became a command group", output)
			}
			selected := locale
			if selected == "fr" {
				selected = "en"
			}
			lookup, err := messages.Lookup(message, selected)
			if err != nil || !lookup.MessageExists || !lookup.TranslationExists || len(lookup.Definition.Forms) != 1 {
				t.Fatal("expected help resource is absent", message, selected, err)
			}
			if envelope.Data.Usage != usage || envelope.Data.Description != lookup.Definition.Forms[0].Pattern {
				t.Fatal("incorrect command path or localized ownership", locale, output)
			}
		}
	}
	for _, args := range [][]string{{"workflow", "check"}, {"workflow", "run"}, {"workflow", "vet"}} {
		output, diagnostic, err := run(args, "en")
		if !errors.Is(err, command.ErrUsage) || command.ExitCode(err) != 2 || output != "" || diagnostic == "" {
			t.Fatal("unimplemented command or missing input was accepted", args, err, output)
		}
	}
}
