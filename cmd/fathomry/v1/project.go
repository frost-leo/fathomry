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
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/config"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/config/check"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/spf13/cobra"
)

// Project supplies compiled project configuration, not a registry or plugin.
// Schema runs once after bootstrap admission and returns detached defaults.
// Callbacks are borrowed, bounded and non-panicking, and remain valid until
// retained cleanup completes. LookupEnv
// captures only BootstrapKeys during config check; nil authorizes no OS lookup.
// Variables are already captured application bindings. No process globals are set.
type Project[T any] struct {
	Schema        configuration.SchemaFactory[T]
	Configuration configuration.Policy
	LookupEnv     func(string) (string, bool)
	Variables     []configuration.Variable
}

// ConfigurationCheckError retains accepted configuration after a failed check.
// The existing ExecutionError remains reachable for cleanup continuation.
type ConfigurationCheckError = check.Error

// ProjectComponents returns the explicit offline metadata for RunProject.
// It includes the common CLI and configuration profile, but not CoreComponents.
func ProjectComponents() []i18n.Component {
	return append(Components(), config.Components()...)
}

// RunProject adds the existing config group/leaf structure to the shared engine.
// It does not launch another program or add a second cleanup owner. The existing
// Resource scope owns configuration work, errors and continued cleanup. A zero CleanupTimeout
// selects a one-minute outer ceiling here; bootstrap controls the inner source
// cleanup budget. A smaller explicit outer ceiling may constrain it further.
// Failures after configuration acceptance return ConfigurationCheckError while
// preserving the existing ExecutionError's ownership and error identity.
func RunProject[T any](ctx context.Context, args []string, project Project[T], options Options) error {
	if options.CleanupTimeout == 0 {
		options.CleanupTimeout = time.Minute
	}
	catalogs := command.Catalogs{Errors: options.Errors, Messages: options.Messages}
	if catalogs.Errors == nil && catalogs.Messages == nil {
		components := append(i18n.CoreComponents(), ProjectComponents()...)
		prepared, err := framework.Prepare(components...)
		if err != nil {
			return command.Unavailable(options.ErrorOutput, err)
		}
		catalogs = command.Catalogs{Errors: prepared.Errors, Messages: prepared.Messages}
	}
	var accepted settings.Reader
	err := command.Run(ctx, args, command.Options{Input: options.Input, Output: options.Output, ErrorOutput: options.ErrorOutput,
		Language: options.Language, CleanupTimeout: options.CleanupTimeout}, catalogs, func(invocation *command.Invocation) *cobra.Command {
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		root.AddCommand(config.New(invocation, check.Declaration[T]{Schema: project.Schema, Policy: project.Configuration, LookupEnv: project.LookupEnv,
			Variables: project.Variables, ExplicitLanguage: options.Language != "", Accepted: &accepted}))
		return root
	})
	return check.Retain(err, accepted)
}
