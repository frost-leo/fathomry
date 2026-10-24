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

package compare

import (
	"bytes"
	"context"
	"errors"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
	"strings"
	"testing"
)

func TestCompareLeafIsOfflineAndLocalized(t *testing.T) {
	calls := 0
	leaf := New(&command.Invocation{}, func(context.Context, workflows.Selection) (workflows.ComparisonReport, error) {
		calls++
		return workflows.ComparisonReport{}, nil
	})
	if leaf.Name() != "compare" || len(leaf.Commands()) != 0 || calls != 0 || leaf.Args(leaf, []string{"bundle.json"}) == nil {
		t.Fatal("leaf performed work")
	}
	messages, err := i18n.Prepare(Component())
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "zh-CN"} {
		selected, err := presenter.WithLocale(locale)
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{leaf.Short, leaf.Flags().Lookup("workflow").Usage, leaf.Flags().Lookup("revision").Usage} {
			shown, err := selected.Render(id, nil, nil)
			if err != nil || shown.Locale != locale || shown.Text == "" {
				t.Fatal("untranslated comparison command", err)
			}
		}
	}
}
func TestCompletedDifferenceAndUnavailableAreDistinct(t *testing.T) {
	components := append(i18n.CoreComponents(), command.Component(), Component())
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	failures, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	for _, unavailable := range []bool{false, true} {
		var output, diagnostic bytes.Buffer
		err := command.Run(context.Background(), []string{"compare", "--output=json"}, command.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}, command.Catalogs{Errors: failures, Messages: messages}, func(invocation *command.Invocation) *cobra.Command {
			root := invocation.Group("fathomry", "fathomry.command_line.root")
			root.AddCommand(New(invocation, func(context.Context, workflows.Selection) (workflows.ComparisonReport, error) {
				if unavailable {
					return workflows.ComparisonReport{}, errors.New("private-database-canary")
				}
				return workflows.ComparisonReport{Format: "fathomry.project-comparison/v1", Checked: true, Complete: true, Equal: false, Workflows: []workflows.WorkflowComparison{}}, nil
			}))
			return root
		})
		if unavailable {
			if err == nil || output.Len() != 0 {
				t.Fatal("unavailable DB emitted completed comparison")
			}
		} else if !errors.Is(err, command.ErrCheck) || command.ExitCode(err) != command.ExitCode(command.ErrCheck) || output.Len() == 0 {
			t.Fatal("completed mismatch became execution failure", err)
		}
		if strings.Contains(output.String()+diagnostic.String(), "private-database") {
			t.Fatal("private failure leaked")
		}
	}
}
