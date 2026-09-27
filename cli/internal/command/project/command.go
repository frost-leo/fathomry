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

package project

import (
	"io"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

// New constructs invocation-local metadata from an explicitly shared catalog.
// All required English contracts are checked before returning a runnable command.
// Source and filesystem inspection occur only during validated execution.
func New(catalog *i18n.Catalog) (*cobra.Command, error) {
	definitions, err := failure.PrepareDefinitions(Definition())
	if err != nil {
		return nil, err
	}
	bindings, err := i18n.PrepareBindings(definitions, catalog, Bindings()...)
	if err != nil {
		return nil, err
	}
	english := make(map[string]string, len(requiredText))
	for _, key := range requiredText {
		found, err := catalog.Lookup("fathomry.cli.project:"+key, "en")
		if err != nil {
			return nil, err
		}
		if !found.TranslationExists || found.Definition.Contract != "v1" ||
			found.Definition.Cardinal || len(found.Definition.Arguments) != 0 {
			return nil, failed(ErrPresentation)
		}
		value, err := renderText(bindings, "en", key)
		if err != nil {
			return nil, err
		}
		english[key] = value
	}
	var input request
	command := &cobra.Command{
		Use: "new <directory>", Short: english["new"],
		Annotations: map[string]string{"fathomry.short.id": "fathomry.cli.project:new"},
	}
	command.Args = func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return failed(ErrArguments)
		}
		input.directory = args[0]
		return input.validate()
	}
	flags := command.Flags()
	flags.StringVar(&input.module, "module", "", english["module"])
	flags.StringVar(&input.source, "fathomry-source", "", english["source"])
	for name, key := range map[string]string{"module": "module", "fathomry-source": "source"} {
		flags.Lookup(name).Annotations = map[string][]string{"fathomry.usage.id": {"fathomry.cli.project:" + key}}
		if err := command.MarkFlagRequired(name); err != nil {
			return nil, err
		}
	}
	command.RunE = func(command *cobra.Command, _ []string) error {
		result := create(command.Context(), input)
		locale := "en"
		if flag := command.Flag("lang"); flag != nil {
			locale = flag.Value.String()
		}
		key, selectionErr := result.presentationKey()
		if selectionErr != nil {
			return combine(result.err, selectionErr)
		}
		writer := command.OutOrStdout()
		if result.err != nil {
			writer = command.ErrOrStderr()
		}
		message, renderErr := renderText(bindings, locale, key)
		if renderErr != nil {
			return combine(result.err, renderErr)
		}
		_, writeErr := io.WriteString(writer, message+"\n")
		return combine(result.err, writeErr)
	}
	return command, nil
}
