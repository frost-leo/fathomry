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

package build

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

// New declares one native build command without acquiring a project.
func New(invocation *command.Invocation) *cobra.Command {
	options := Options{Directory: "."}
	cmd := &cobra.Command{Use: "build [PROJECT]", Short: "fathomry.project_build.command", Args: cobra.MaximumNArgs(1)}
	cmd.Flags().StringVar(&options.Output, "out", "", "fathomry.project_build.out")
	cmd.Flags().DurationVar(&options.Timeout, "timeout", DefaultTimeout, "fathomry.project_build.timeout")
	cmd.Flags().BoolVar(&options.DeclarationOnly, "declaration-only", false, "fathomry.project_build.declaration_only")
	invocation.Bind(cmd, func(ctx context.Context, args []string) error {
		if len(args) == 1 {
			options.Directory = args[0]
		}
		result, err := Run(ctx, options)
		if err != nil {
			return err
		}
		return invocation.Result("build", result, func(writer io.Writer) error {
			return Render(writer, result, invocation.Locale())
		})
	})
	return cmd
}

func presenter(language string) (i18n.Presenter, error) {
	if language == "" {
		language = "en"
	}
	if len(language) > 128 {
		return i18n.Presenter{}, fail(ErrOptions)
	}
	catalog, err := i18n.Prepare(Component())
	if err != nil {
		return i18n.Presenter{}, err
	}
	value, err := i18n.NewPresenter(catalog)
	if err != nil {
		return i18n.Presenter{}, err
	}
	return value.WithLocale(language)
}

// Render writes a minimal localized text banner. It emits no ANSI control codes;
// JSON consumers must encode Result instead. Writer failure can follow publication.
func Render(writer io.Writer, result Result, language string) error {
	if writer == nil || !result.Published || !validName(result.Name) {
		return fail(ErrOptions)
	}
	value, err := presenter(language)
	if err != nil {
		return err
	}
	labels := make([]string, 6)
	for index, name := range []string{"complete", "project", "target", "declarations", "artifact", "digest"} {
		message, err := value.Render("fathomry.project_build."+name, nil, nil)
		if err != nil {
			return err
		}
		labels[index] = message.Text
	}
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "\n  F A T H O M R Y\n  ────────────────────────────────────────────────\n  %s\n\n", labels[0])
	fmt.Fprintf(&buffer, "  %-14s %s\n  %-14s %q\n  %-14s %d / %d\n  %-14s %q\n  %-14s %q\n\n",
		labels[1], result.Name, labels[2], result.Target, labels[3], result.Workflows, result.Modes,
		labels[4], result.Artifact, labels[5], result.SHA256)
	messageID := "implementation_coverage"
	if result.Inspection != implementationInspectionSchema {
		messageID = "implementation_unavailable"
	}
	message, err := value.Render("fathomry.project_build."+messageID, nil, nil)
	if err != nil {
		return err
	}
	complete := 0
	for _, record := range result.Implementations {
		if record.Status == "complete" {
			complete++
		}
	}
	if result.Inspection == implementationInspectionSchema {
		fmt.Fprintf(&buffer, "  %s: %d / %d (%s)\n", message.Text, complete, len(result.Implementations), result.Inspection)
	} else {
		fmt.Fprintf(&buffer, "  %s (%s)\n", message.Text, result.Inspection)
	}
	return writeAll(writer, buffer.Bytes())
}
