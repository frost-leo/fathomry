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

package register

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

func TestRegisterLeafIsLazyAndLocalized(t *testing.T) {
	calls := 0
	run := func(context.Context, workflows.Selection) (workflows.PublicationReport, error) {
		calls++
		return workflows.PublicationReport{}, nil
	}
	leaf := New(&command.Invocation{}, run)
	if calls != 0 || len(leaf.Commands()) != 0 || leaf.Name() != "register" || leaf.Args(leaf, []string{"bundle.json"}) == nil {
		t.Fatal("leaf layout acquired execution")
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
		for _, id := range []string{leaf.Short, leaf.Flags().Lookup("namespace").Usage, leaf.Flags().Lookup("workflow").Usage, leaf.Flags().Lookup("revision").Usage} {
			result, err := selected.Render(id, nil, nil)
			if err != nil || result.Locale != locale || result.Text == "" {
				t.Fatal("missing localization", err)
			}
		}
	}
}

type brokenOutput struct{}

func (brokenOutput) Write([]byte) (int, error) { return 0, errors.New("controlled output failure") }
func TestRegisterMutationFactsSurviveOutputFailure(t *testing.T) {
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
	var diagnostic bytes.Buffer
	result := workflows.PublicationReport{Format: "fathomry.project-publication/v1", Checked: true, Selected: 1, Attempted: 1, Complete: true, Effect: "applied", Workflows: []workflows.WorkflowPublication{}}
	err = command.Run(context.Background(), []string{"register", "--output=json"}, command.Options{Input: strings.NewReader(""), Output: brokenOutput{}, ErrorOutput: &diagnostic}, command.Catalogs{Errors: failures, Messages: messages}, func(invocation *command.Invocation) *cobra.Command {
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		root.AddCommand(New(invocation, func(context.Context, workflows.Selection) (workflows.PublicationReport, error) { return result, nil }))
		return root
	})
	var retained *command.ExecutionError
	if !errors.As(err, &retained) || !errors.Is(err, command.ErrOutput) || !bytes.Contains(retained.ReportCopy(), []byte("\"effect\":\"applied\"")) {
		t.Fatal("output failure erased known mutation", err)
	}
	if !retained.CleanupComplete() {
		t.Fatal("data-only leaf acquired resources")
	}
}
