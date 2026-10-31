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

package cli

import (
	"context"
	"io"
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

// Options supplies explicit invocation dependencies. All streams are required.
// Empty Language selects English. Run uses a five-second default cleanup budget;
// RunProject documents its configuration profile's outer cleanup ceiling.
// Errors and Messages must either both be nil or include Components and the
// caller's chosen component metadata.
type Options struct {
	Input          io.Reader
	Output         io.Writer
	ErrorOutput    io.Writer
	Language       string
	CleanupTimeout time.Duration
	Errors         *failure.Catalog
	Messages       *i18n.Catalog
}

// ExecutionError retains mutation facts and unfinished resource cleanup.
type ExecutionError = command.ExecutionError

// Components returns only the retained CLI foundation's offline metadata.
func Components() []i18n.Component {
	return []i18n.Component{command.Component()}
}

// Run executes the minimal CLI. Only help and global language/output flags are
// supported. Withdrawn commands and project inspection protocols are rejected.
func Run(ctx context.Context, args []string, options Options) error {
	catalogs := command.Catalogs{Errors: options.Errors, Messages: options.Messages}
	if catalogs.Errors == nil && catalogs.Messages == nil {
		components := append(i18n.CoreComponents(), Components()...)
		var definitions []failure.Definition
		for _, component := range components {
			definitions = append(definitions, component.Definitions...)
		}
		var err error
		catalogs.Errors, err = failure.Prepare(definitions...)
		if err == nil {
			catalogs.Messages, err = i18n.Prepare(components...)
		}
		if err != nil {
			return command.Unavailable(options.ErrorOutput, err)
		}
	}
	return command.Run(ctx, args, command.Options{
		Input: options.Input, Output: options.Output, ErrorOutput: options.ErrorOutput,
		Language: options.Language, CleanupTimeout: options.CleanupTimeout,
	}, catalogs, func(invocation *command.Invocation) *cobra.Command {
		return invocation.Group("fathomry", "fathomry.command_line.root")
	})
}

// ExitCode applies the official executable's process policy.
func ExitCode(err error) int { return command.ExitCode(err) }
