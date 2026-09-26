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
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func commands(words *text) *cobra.Command {
	root := &cobra.Command{Use: "fathomry", Short: words.english["root"], Args: cobra.NoArgs}
	root.Annotations = map[string]string{"fathomry.short.zh-CN": words.chinese["root"]}
	return root
}

func prepare(root *cobra.Command, words *text) error {
	if root == nil || root.RunE != nil || pflag.CommandLine.HasFlags() {
		return errDefinition
	}
	language := &languageValue{words: words}
	persistent := root.PersistentFlags()
	if persistent.Lookup("lang") != nil || persistent.GetNormalizeFunc()(persistent, "lang") != "lang" ||
		persistent.GetNormalizeFunc()(persistent, "help") != "help" {
		return errDefinition
	}
	persistent.Var(language, "lang", words.english["language"])
	persistent.Lookup("lang").Annotations = map[string][]string{
		"fathomry.usage.zh-CN": {words.chinese["language"]},
	}
	if err := prepareCommand(root, nil, nil); err != nil {
		return err
	}
	help := &cobra.Command{Use: "help", Short: words.english["help"], Args: cobra.ArbitraryArgs,
		Annotations: map[string]string{"fathomry.short.zh-CN": words.chinese["help"]}}
	if err := prepareCommand(help, nil, nil); err != nil {
		return err
	}
	root.AddCommand(help)
	persistent.BoolP("help", "h", false, "")
	return nil
}

func prepareCommand(command *cobra.Command, inheritedNames, inheritedShorts map[string]*pflag.Flag) error {
	if command.Args == nil || command.Run != nil || command.DisableFlagParsing ||
		command.RunE != nil && command.HasSubCommands() ||
		command.PreRun != nil || command.PreRunE != nil || command.PostRun != nil ||
		command.PostRunE != nil || command.PersistentPreRun != nil || command.PersistentPreRunE != nil ||
		command.PersistentPostRun != nil || command.PersistentPostRunE != nil ||
		command.FParseErrWhitelist.UnknownFlags || command.Deprecated != "" {
		return errDefinition
	}
	flags := command.Flags()
	if flags.Parsed() || command.PersistentFlags().Parsed() ||
		flags.ParseErrorsWhitelist.UnknownFlags || flags.ParseErrorsAllowlist.UnknownFlags {
		return errDefinition
	}
	// Native helper sets reuse the saved global normalizer and mutate shared Flag.Name.
	normalize, globalNormalize := flags.GetNormalizeFunc(), command.GlobalNormalizationFunc()
	preservesName := func(name string) bool {
		return normalize(flags, name) == pflag.NormalizedName(name) &&
			(globalNormalize == nil || globalNormalize(flags, name) == pflag.NormalizedName(name))
	}
	names, shorts := copyNames(inheritedNames), copyNames(inheritedShorts)
	invalid := !preservesName("help") || !preservesName("lang")
	check := func(flag *pflag.Flag) {
		if !preservesName(flag.Name) || names[flag.Name] != nil && names[flag.Name] != flag ||
			flag.Name == "help" || flag.Shorthand == "h" ||
			flag.Deprecated != "" || flag.ShorthandDeprecated != "" {
			invalid = true
		}
		names[flag.Name] = flag
		if flag.Shorthand != "" {
			if shorts[flag.Shorthand] != nil && shorts[flag.Shorthand] != flag {
				invalid = true
			}
			shorts[flag.Shorthand] = flag
		}
	}
	visit := func(set *pflag.FlagSet) {
		set.VisitAll(func(flag *pflag.Flag) {
			if set.Lookup(flag.Name) != flag {
				invalid = true
			}
			check(flag)
		})
	}
	for _, flag := range inheritedNames {
		check(flag)
	}
	visit(command.PersistentFlags())
	childNames, childShorts := copyNames(names), copyNames(shorts)
	if command.Parent() == nil || command.HasSubCommands() {
		flags.VisitAll(func(flag *pflag.Flag) {
			if names[flag.Name] != flag {
				invalid = true
			}
		})
	}
	visit(flags)
	if invalid {
		return errDefinition
	}
	command.SilenceErrors, command.SilenceUsage = true, true
	command.DisableSuggestions = true
	command.CompletionOptions.DisableDefaultCmd = true
	siblings := map[string]bool{}
	for _, child := range command.Commands() {
		for _, name := range append([]string{child.Name()}, child.Aliases...) {
			if name == "" || strings.ContainsAny(name, " \t\r\n") || siblings[name] || reserved(name) {
				return errDefinition
			}
			siblings[name] = true
		}
		if err := prepareCommand(child, childNames, childShorts); err != nil {
			return err
		}
	}
	return nil
}

func reserved(name string) bool {
	return name == "help" || name == "completion" || name == "__complete" || name == "__completeNoDesc"
}

func copyNames(source map[string]*pflag.Flag) map[string]*pflag.Flag {
	result := make(map[string]*pflag.Flag, len(source))
	for name, value := range source {
		result[name] = value
	}
	return result
}
