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
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func shortText(command *cobra.Command, words *text) (string, error) {
	if id, exists := command.Annotations["fathomry.short.id"]; exists {
		return words.render(id, words.language)
	}
	// Legacy raw metadata is confined to private/test-only command families.
	if words.language == "zh-CN" && command.Annotations["fathomry.short.zh-CN"] != "" {
		return command.Annotations["fathomry.short.zh-CN"], nil
	}
	return command.Short, nil
}

func renderHelp(writer io.Writer, command *cobra.Command, words *text) error {
	description, err := shortText(command, words)
	if err != nil {
		return err
	}
	usage := command.CommandPath() + strings.TrimPrefix(command.Use, command.Name())
	heading := words.get("usage")
	if err := words.failure(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "%s\n\n%s: %s\n", description, heading, usage); err != nil {
		return err
	}
	headingWritten := false
	for _, child := range command.Commands() {
		if child.Hidden {
			continue
		}
		description, err := shortText(child, words)
		if err != nil {
			return err
		}
		if !headingWritten {
			heading := words.get("commands")
			if err := words.failure(); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(writer, "\n%s:\n", heading); err != nil {
				return err
			}
			headingWritten = true
		}
		if _, err := fmt.Fprintf(writer, "  %s\t%s\n", child.Name(), description); err != nil {
			return err
		}
	}
	heading = words.get("flags")
	if err := words.failure(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "\n%s:\n", heading); err != nil {
		return err
	}
	var writeErr error
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.AddFlagSet(command.LocalFlags())
	flags.AddFlagSet(command.InheritedFlags())
	flags.VisitAll(func(flag *pflag.Flag) {
		if flag.Hidden || writeErr != nil {
			return
		}
		description := flag.Usage
		if ids, exists := flag.Annotations["fathomry.usage.id"]; exists {
			if len(ids) != 1 || ids[0] == "" {
				writeErr = words.retain(hostFailure(ErrDefinition))
				return
			}
			description, writeErr = words.render(ids[0], words.language)
			if writeErr != nil {
				return
			}
		} else if words.language == "zh-CN" {
			if translated := flag.Annotations["fathomry.usage.zh-CN"]; len(translated) != 0 && translated[0] != "" {
				description = translated[0]
			}
		}
		if writeErr = words.failure(); writeErr != nil {
			return
		}
		name := "--" + flag.Name
		if flag.Shorthand != "" {
			name = "-" + flag.Shorthand + ", " + name
		}
		if flag.NoOptDefVal == "" {
			name += " <" + flag.Value.Type() + ">"
		}
		_, writeErr = fmt.Fprintf(writer, "  %s\t%s\n", name, description)
	})
	return writeErr
}
