/*
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
	"errors"
	"path/filepath"
	"runtime/debug"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/cli"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/i18n"
	"github.com/spf13/cobra"
)

const (
	HelpID    = "fathomry.cli.new.help"
	SummaryID = "fathomry.cli.new.summary"
)

// New declares the creation command without inspecting builds, files or services.
func New(call *cli.Invocation) *cobra.Command {
	var options Options
	directory := "."
	command := &cobra.Command{Use: "new <name>", Args: cobra.ExactArgs(1)}
	command.Flags().StringVar(&directory, "directory", ".", "")
	command.Flags().StringVar(&options.Module, "module", "", "")
	command.Flags().StringVar(&options.FrameworkVersion, "framework-version", "", "")
	command.Flags().StringVar(&options.Configuration, "configuration", "local", "")
	command.Flags().StringVar(&options.Provider, "provider", "", "")
	command.Flags().StringArrayVar(&options.EnvironmentSources, "environment-source", nil, "")
	command.Flags().StringVar(&options.DefaultLocale, "default-locale", "en", "")
	command.Flags().StringVar(&options.Bundle, "sdk-bundle", "", "")
	command.RunE = func(command *cobra.Command, args []string) error {
		if err := call.ValidateLocale(); err != nil {
			return err
		}
		if err := call.CheckContext(); err != nil {
			return err
		}
		options.Name = args[0]
		plan, err := selectSources(options)
		if err != nil {
			return cli.Usage()
		}
		if options.FrameworkVersion == "" {
			options.FrameworkVersion = defaultVersion()
		}
		if !name(options.Name) || options.Module == "" || options.FrameworkVersion == "" {
			return cli.Usage()
		}
		parent, err := filepath.Abs(directory)
		if err != nil {
			return cli.Usage()
		}
		options.Directory = filepath.Join(parent, options.Name)
		if options.Bundle != "" {
			options.Bundle, err = filepath.Abs(options.Bundle)
			if err != nil {
				return cli.Usage()
			}
		}
		catalog, err := call.Catalog(Resources()...)
		if err != nil {
			return cli.Render(err)
		}
		result, err := Create(command.Context(), options)
		if err != nil {
			if contextErr := call.CheckContext(); contextErr != nil {
				return contextErr
			}
			if errors.Is(err, InvalidInput) {
				return cli.Usage()
			}
			code := Unavailable
			if inspected, ok := failure.Inspect(err); ok {
				code = inspected.Code()
			}
			return cli.Execution(code, err, Resources()...)
		}
		if !result.Complete {
			return cli.Execution(Incomplete, nil, Resources()...)
		}
		rendered, err := catalog.Render(call.Locale(), "fathomry.cli.new.created", i18n.Arguments{
			"Name": options.Name, "Module": options.Module, "Provider": plan.Mode + "/" + plan.Provider, "Version": options.FrameworkVersion,
		})
		if err != nil {
			return cli.Render(err)
		}
		return call.WriteFinite(rendered.Text+"\n", 16<<10)
	}
	return command
}

func defaultVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	if info.Main.Path == FrameworkModule && info.Main.Replace == nil && frameworkVersion(info.Main.Version) {
		return info.Main.Version
	}
	return ""
}
