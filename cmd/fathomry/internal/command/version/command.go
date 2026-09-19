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

package version

import (
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	"github.com/frost-leo/fathomry/failure"
	buildversion "github.com/frost-leo/fathomry/version"
	"github.com/spf13/cobra"
)

// HelpID and SummaryID are the command-owned localized descriptions.
const (
	HelpID    = "fathomry.cli.version.help"
	SummaryID = "fathomry.cli.version.summary"
)

// New constructs the version command without reading build metadata or services.
func New(call *cli.Invocation) *cobra.Command {
	output := "text"
	command := &cobra.Command{
		Use: "version", Args: cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error { return Report(call, output) },
	}
	command.Flags().StringVar(&output, "output", "text", "")
	return command
}

// Report is the shared operation for the subcommand and root --version flag.
// It validates before inspecting; help never needs to inspect a declaration.
func Report(call *cli.Invocation, output string) error {
	return report(call, output, buildversion.Inspect)
}

func report(call *cli.Invocation, output string, inspect func(buildversion.Request) (buildversion.Build, error)) error {
	if output != "text" && output != "json" {
		return cli.Usage()
	}
	if err := call.ValidateLocale(); err != nil {
		return err
	}
	if err := call.CheckContext(); err != nil {
		return err
	}
	build, err := inspect(buildversion.Request{})
	if err != nil {
		if described, ok := failure.Inspect(err); ok {
			return cli.Execution(described.Code(), err, Resources()...)
		}
		return cli.Render(err)
	}
	var rendered string
	if output == "json" {
		rendered, err = versionJSON(build)
	} else {
		var catalogErr error
		catalog, prepareErr := call.Catalog(Resources()...)
		if prepareErr != nil {
			catalogErr = prepareErr
		} else {
			rendered, catalogErr = versionText(catalog, call.Locale(), build)
		}
		err = catalogErr
	}
	if err != nil {
		return cli.Render(err)
	}
	return call.WriteFinite(rendered, 16<<10)
}
