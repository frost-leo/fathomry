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

package check

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	"github.com/spf13/cobra"
)

// Declaration borrows a bounded project schema and optional process lookup.
// The invocation's owner retains their validity until cleanup is complete.
// Accepted is an optional invocation-owned result location, written before Apply.
// Its read-only data cell retains acceptance independently of lease acquisition.
type Declaration[T any] struct {
	Schema           configuration.SchemaFactory[T]
	Policy           configuration.Policy
	LookupEnv        func(string) (string, bool)
	Variables        []configuration.Variable
	ExplicitLanguage bool
	Accepted         *settings.Reader
}

// New constructs arguments and a handler, not configuration or clients.
func New[T any](invocation *command.Invocation, declaration Declaration[T]) *cobra.Command {
	var path, environment string
	leaf := &cobra.Command{Use: "check", Short: "fathomry.configuration_check.check", Args: cobra.NoArgs}
	leaf.Flags().StringVar(&path, "bootstrap", ".env", "fathomry.configuration_check.bootstrap")
	leaf.Flags().StringVar(&environment, "env", "", "fathomry.configuration_check.environment")
	invocation.Bind(leaf, func(ctx context.Context, _ []string) error {
		if path == "" {
			return command.Fail(command.ErrUsage)
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return command.Fail(command.ErrUsage, err)
		}
		input := configuration.BootstrapInput{Path: absolute, Variables: map[string]string{}}
		if leaf.Flags().Changed("env") {
			input.Environment = &environment
		}
		if declaration.LookupEnv != nil {
			for _, key := range configuration.BootstrapKeys() {
				if err := ctx.Err(); err != nil {
					return err
				}
				if value, present := declaration.LookupEnv(key); present {
					input.Variables[key] = value
				}
			}
		}
		prepared, err := configuration.PrepareInput(ctx, input, declaration.Variables)
		if err != nil {
			return err
		}
		invocation.Managed()
		scope, err := invocation.Scope()
		if err != nil {
			return err
		}
		ref, accepted, err := configuration.BindConfiguration(scope, "configuration", declaration.Schema, declaration.Policy)
		if err != nil {
			return err
		}
		if declaration.Accepted != nil {
			*declaration.Accepted = accepted
		}
		update, err := scope.Apply(ctx, prepared.View())
		if err != nil {
			return err
		}
		if err := update.Wait(ctx); err != nil {
			return err
		}
		lease, err := ref.Acquire(ctx)
		if err != nil {
			return err
		}
		return func() (result error) {
			defer func() {
				if err := lease.Release(); err != nil {
					result = errors.Join(result, err)
				}
			}()
			loaded, err := lease.Value()
			if err != nil {
				return err
			}
			view, err := loaded.InitialView()
			if err != nil {
				return err
			}
			info, err := loaded.Info()
			if err != nil {
				return err
			}
			selected, err := report(view, info)
			if err != nil {
				return err
			}
			locale := selected.ApplicationLocale
			if declaration.ExplicitLanguage || leaf.Root().PersistentFlags().Changed("lang") {
				locale = invocation.Locale()
			}
			presenter, err := i18n.NewPresenter(invocation.Catalogs().Messages)
			if err != nil {
				return err
			}
			presenter, err = presenter.WithLocale(locale)
			if err != nil {
				return err
			}
			selected.OutputLocale = locale
			return invocation.CheckResult("config.check", selected, true, func(writer io.Writer) error {
				text, err := presenter.Render("fathomry.configuration_check.accepted", nil, nil)
				if err != nil {
					return err
				}
				_, err = fmt.Fprintf(writer, "%s: %s [%s; %s/%s]\n", text.Text, selected.Application, selected.Environment, selected.Mode, selected.Provider)
				return err
			})
		}()
	})
	return leaf
}

type checkReport struct {
	Accepted          bool             `json:"accepted"`
	Environment       string           `json:"environment"`
	Mode              string           `json:"mode"`
	Provider          string           `json:"provider"`
	Watching          bool             `json:"watching"`
	SchemaVersion     uint32           `json:"schema_version"`
	Application       string           `json:"application"`
	ApplicationLocale string           `json:"application_locale"`
	OutputLocale      string           `json:"output_locale"`
	Documents         []documentReport `json:"documents"`
}
type documentReport struct {
	Kind     string `json:"kind"`
	Encoding string `json:"encoding"`
}

func report(view settings.View, info configuration.Info) (checkReport, error) {
	name, present, err := settings.Read(view, "/application/name", func(value string) string { return value })
	if err != nil || !present || !validName(name) {
		return checkReport{}, command.Fail(command.ErrExecution, err)
	}
	preferences, present, err := settings.Read(view, i18n.PreferencesPath, func(value i18n.Preferences) i18n.Preferences { return value })
	if err != nil || !present {
		return checkReport{}, command.Fail(command.ErrExecution, err)
	}
	locale, err := i18n.NormalizeLocale(preferences.Locale)
	if err != nil {
		return checkReport{}, err
	}
	result := checkReport{Accepted: true, Environment: info.Environment, Mode: info.Mode, Provider: info.Provider, Watching: info.Watching,
		SchemaVersion: info.SchemaVersion, Application: name, ApplicationLocale: locale, Documents: make([]documentReport, 0, len(info.Documents))}
	for _, document := range info.Documents {
		result.Documents = append(result.Documents, documentReport{Kind: document.Kind, Encoding: string(document.Encoding)})
	}
	return result, nil
}
func validName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '.' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}
