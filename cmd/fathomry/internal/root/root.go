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
	"context"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	versioncmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/version"
	"github.com/spf13/cobra"
)

// Run executes the explicitly composed CLI with caller-owned inputs and streams.
func Run(ctx context.Context, args []string, in io.Reader, out, errOut io.Writer) int {
	return cli.Run(ctx, args, in, out, errOut, New)
}

// New constructs one command tree without opening configuration or clients.
func New(call *cli.Invocation) *cobra.Command {
	root := call.NewRoot(cli.RootText{
		HeaderID: "fathomry.cli.root.header", FooterID: "fathomry.cli.root.footer",
		Resources: Resources(),
	})
	var showVersion bool
	output := "text"
	root.Flags().BoolVar(&showVersion, "version", false, "")
	root.Flags().StringVar(&output, "output", "text", "")
	root.RunE = func(command *cobra.Command, _ []string) error {
		if err := call.ValidateLocale(); err != nil {
			return err
		}
		if !showVersion {
			if command.Flags().Changed("output") {
				return cli.Usage()
			}
			return call.Help(root)
		}
		return versioncmd.Report(call, output)
	}
	call.Add(root, versioncmd.New(call), versioncmd.HelpID, versioncmd.SummaryID, versioncmd.Resources()...)
	return root
}
