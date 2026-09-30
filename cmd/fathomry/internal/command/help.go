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

package command

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type helpEntry struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}
type helpFlag struct {
	Name        string `json:"name"`
	Shorthand   string `json:"shorthand,omitempty"`
	Type        string `json:"type"`
	Default     string `json:"default"`
	Description string `json:"description"`
}
type helpDocument struct {
	Usage       string      `json:"usage"`
	Description string      `json:"description"`
	Commands    []helpEntry `json:"commands"`
	Flags       []helpFlag  `json:"flags"`
}

// Help never calls SDK templates or process-exiting helpers.
func (invocation *Invocation) Help(cmd *cobra.Command) error {
	if err := invocation.configure(); err != nil {
		return err
	}
	usage := cmd.CommandPath()
	if _, suffix, found := strings.Cut(cmd.Use, " "); found {
		usage += " " + suffix
	}
	value := helpDocument{Usage: usage, Description: invocation.Text(cmd.Short), Commands: []helpEntry{}, Flags: []helpFlag{}}
	for _, child := range cmd.Commands() {
		if child.Hidden {
			continue
		}
		value.Commands = append(value.Commands, helpEntry{Name: child.Name(), Description: invocation.Text(child.Short)})
	}
	add := func(flag *pflag.Flag) {
		if flag.Hidden {
			return
		}
		value.Flags = append(value.Flags, helpFlag{Name: flag.Name, Shorthand: flag.Shorthand,
			Type: flag.Value.Type(), Default: flag.DefValue, Description: invocation.Text(flag.Usage)})
	}
	cmd.LocalFlags().VisitAll(add)
	cmd.InheritedFlags().VisitAll(add)
	return invocation.Result("help", value, func(writer io.Writer) error {
		if _, err := fmt.Fprintf(writer, "%s\n\n%s %s\n", value.Description, invocation.Text("fathomry.command_line.usage"), value.Usage); err != nil {
			return err
		}
		if len(value.Commands) > 0 {
			if _, err := fmt.Fprintf(writer, "\n%s\n", invocation.Text("fathomry.command_line.commands")); err != nil {
				return err
			}
			for _, child := range value.Commands {
				if _, err := fmt.Fprintf(writer, "  %s  %s\n", child.Name, child.Description); err != nil {
					return err
				}
			}
		}
		if len(value.Flags) > 0 {
			if _, err := fmt.Fprintf(writer, "\n%s\n", invocation.Text("fathomry.command_line.options")); err != nil {
				return err
			}
			for _, flag := range value.Flags {
				name := "--" + flag.Name
				if flag.Shorthand != "" {
					name = "-" + flag.Shorthand + ", " + name
				}
				if _, err := fmt.Fprintf(writer, "  %s  %s\n", name, flag.Description); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
