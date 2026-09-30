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

// Package messages implements offline translation-resource inspection.
package messages

import (
	"context"
	"fmt"
	"io"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

type parameter struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Meaning string `json:"meaning"`
}
type form struct {
	Category string `json:"category"`
	Pattern  string `json:"pattern"`
}
type entry struct {
	Module       string      `json:"module"`
	Component    string      `json:"component"`
	ID           string      `json:"id"`
	Locale       string      `json:"locale"`
	BaseLocale   string      `json:"base_locale"`
	Contract     string      `json:"contract"`
	Context      string      `json:"context"`
	SourceDigest string      `json:"source_digest"`
	Cardinal     bool        `json:"cardinal"`
	Arguments    []parameter `json:"arguments"`
	Forms        []form      `json:"forms"`
}

func describe(value i18n.Definition) entry {
	result := entry{Module: value.Module, Component: value.Component, ID: value.ID, Locale: value.Locale,
		BaseLocale: value.BaseLocale, Contract: value.Contract, Context: value.Context,
		SourceDigest: value.SourceDigest, Cardinal: value.Cardinal, Arguments: []parameter{}, Forms: []form{}}
	for _, argument := range value.Arguments {
		result.Arguments = append(result.Arguments, parameter{argument.Name, argument.Kind, argument.Meaning})
	}
	for _, variant := range value.Forms {
		result.Forms = append(result.Forms, form{string(variant.Category), variant.Pattern})
	}
	return result
}

type lookup struct {
	MessageExists     bool   `json:"message_exists"`
	TranslationExists bool   `json:"translation_exists"`
	Definition        *entry `json:"definition,omitempty"`
}
type coverage struct {
	Module    string   `json:"module"`
	Component string   `json:"component"`
	Locale    string   `json:"locale"`
	Missing   []string `json:"missing"`
}

// New binds the immutable atlas supplied by the process assembly.
func New(invocation *command.Invocation) *cobra.Command {
	root := invocation.Group("i18n", "fathomry.command_line.i18n_group")
	list := &cobra.Command{Use: "list", Short: "fathomry.command_line.i18n_list", Args: cobra.NoArgs}
	invocation.Bind(list, func(ctx context.Context, _ []string) error {
		values, err := invocation.Catalogs().Messages.Inspect()
		if err != nil {
			return err
		}
		entries := make([]entry, 0, len(values))
		for _, value := range values {
			if err := ctx.Err(); err != nil {
				return err
			}
			entries = append(entries, describe(value))
		}
		return invocation.Result("i18n.list", entries, func(writer io.Writer) error {
			for _, value := range entries {
				if _, err := fmt.Fprintf(writer, "%s  %s\n", value.ID, value.Locale); err != nil {
					return err
				}
			}
			return nil
		})
	})
	var locale string
	show := &cobra.Command{Use: "show MESSAGE_ID", Short: "fathomry.command_line.i18n_show", Args: cobra.ExactArgs(1)}
	show.Flags().StringVar(&locale, "locale", "", "fathomry.command_line.flag_locale")
	invocation.Bind(show, func(_ context.Context, args []string) error {
		selected := locale
		if selected == "" {
			selected = invocation.Locale()
		}
		value, err := invocation.Catalogs().Messages.Lookup(args[0], selected)
		if err != nil {
			return command.Fail(command.ErrUsage, err)
		}
		result := lookup{MessageExists: value.MessageExists, TranslationExists: value.TranslationExists}
		if !value.MessageExists {
			return command.Fail(command.ErrNotFound)
		}
		if value.TranslationExists {
			item := describe(value.Definition)
			result.Definition = &item
		}
		return invocation.Result("i18n.show", result, func(writer io.Writer) error {
			if !value.TranslationExists {
				_, err := fmt.Fprintln(writer, invocation.Text("fathomry.command_line.missing_translation"))
				return err
			}
			for _, variant := range value.Definition.Forms {
				if _, err := fmt.Fprintf(writer, "%s  %s  %s\n", value.Definition.Locale, variant.Category, variant.Pattern); err != nil {
					return err
				}
			}
			return nil
		})
	})
	check := &cobra.Command{Use: "coverage LOCALE", Short: "fathomry.command_line.i18n_coverage", Args: cobra.ExactArgs(1)}
	invocation.Bind(check, func(_ context.Context, args []string) error {
		values, err := invocation.Catalogs().Messages.Coverage(args[0])
		if err != nil {
			return command.Fail(command.ErrUsage, err)
		}
		result := make([]coverage, 0, len(values))
		for _, value := range values {
			result = append(result, coverage{Module: value.Module, Component: value.Component, Locale: value.Locale, Missing: append([]string{}, value.Missing...)})
		}
		return invocation.Result("i18n.coverage", result, func(writer io.Writer) error {
			for _, value := range result {
				if _, err := fmt.Fprintf(writer, "%s.%s  %s  %s: %d\n", value.Module, value.Component, value.Locale,
					invocation.Text("fathomry.command_line.missing"), len(value.Missing)); err != nil {
					return err
				}
				for _, id := range value.Missing {
					if _, err := fmt.Fprintf(writer, "  %s\n", id); err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
	root.AddCommand(list, show, check)
	return root
}
