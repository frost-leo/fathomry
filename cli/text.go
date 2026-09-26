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
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

//go:embed resources/*.json
var resources embed.FS

type text struct {
	language string
	english  map[string]string
	chinese  map[string]string
}

func newText() *text {
	load := func(path string) map[string]string {
		data, err := resources.ReadFile(path)
		if err != nil {
			panic(err)
		}
		var resource struct{ Messages map[string]string }
		if err := json.Unmarshal(data, &resource); err != nil {
			panic(err)
		}
		return resource.Messages
	}
	return &text{language: "en", english: load("resources/en.json"), chinese: load("resources/zh-cn.json")}
}

func (words *text) get(key string) string {
	if words.language == "zh-CN" && words.chinese[key] != "" {
		return words.chinese[key]
	}
	return words.english[key]
}

type languageValue struct{ words *text }

func (value *languageValue) String() string { return value.words.language }
func (value *languageValue) Type() string   { return "string" }
func (value *languageValue) Set(input string) error {
	switch {
	case strings.EqualFold(input, "en"):
		value.words.language = "en"
	case strings.EqualFold(input, "zh-CN"):
		value.words.language = "zh-CN"
	default:
		return errLanguage
	}
	return nil
}

func shortText(command *cobra.Command, words *text) string {
	if words.language == "zh-CN" && command.Annotations["fathomry.short.zh-CN"] != "" {
		return command.Annotations["fathomry.short.zh-CN"]
	}
	return command.Short
}

func renderHelp(writer io.Writer, command *cobra.Command, words *text) error {
	usage := command.CommandPath() + strings.TrimPrefix(command.Use, command.Name())
	if _, err := fmt.Fprintf(writer, "%s\n\n%s: %s\n", shortText(command, words), words.get("usage"), usage); err != nil {
		return err
	}
	headingWritten := false
	for _, child := range command.Commands() {
		if child.Hidden {
			continue
		}
		if !headingWritten {
			if _, err := fmt.Fprintf(writer, "\n%s:\n", words.get("commands")); err != nil {
				return err
			}
			headingWritten = true
		}
		if _, err := fmt.Fprintf(writer, "  %s\t%s\n", child.Name(), shortText(child, words)); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(writer, "\n%s:\n", words.get("flags")); err != nil {
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
		if words.language == "zh-CN" {
			if translated := flag.Annotations["fathomry.usage.zh-CN"]; len(translated) != 0 && translated[0] != "" {
				description = translated[0]
			}
		}
		if flag.Name == "help" {
			description = words.get("helpFlag")
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
