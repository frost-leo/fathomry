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

	"github.com/frost-leo/fathomry/cli/internal/command/project"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func commands(words *text) *cobra.Command {
	root := &cobra.Command{Use: "fathomry", Short: words.english("root"), Args: cobra.NoArgs}
	root.Annotations = map[string]string{"fathomry.short.id": "fathomry.cli:root"}
	command, err := project.New(words.catalog)
	if err != nil {
		words.retain(err)
	} else {
		root.AddCommand(command)
	}
	return root
}

func prepare(root *cobra.Command, words *text) error {
	if err := words.validate(); err != nil {
		return err
	}
	if root == nil || root.RunE != nil || pflag.CommandLine.HasFlags() {
		return hostFailure(ErrDefinition)
	}
	language := &languageValue{words: words}
	persistent := root.PersistentFlags()
	if persistent.Lookup("lang") != nil || persistent.GetNormalizeFunc()(persistent, "lang") != "lang" ||
		persistent.GetNormalizeFunc()(persistent, "help") != "help" {
		return hostFailure(ErrDefinition)
	}
	persistent.Var(language, "lang", words.english("language"))
	persistent.Lookup("lang").Annotations = map[string][]string{
		"fathomry.usage.id": {"fathomry.cli:language"},
	}
	if err := prepareCommand(root, nil, nil); err != nil {
		return err
	}
	help := &cobra.Command{Use: "help", Short: words.english("help"), Args: cobra.ArbitraryArgs,
		Annotations: map[string]string{"fathomry.short.id": "fathomry.cli:help"}}
	if err := prepareCommand(help, nil, nil); err != nil {
		return err
	}
	root.AddCommand(help)
	persistent.BoolP("help", "h", false, words.english("helpFlag"))
	persistent.Lookup("help").Annotations = map[string][]string{
		"fathomry.usage.id": {"fathomry.cli:helpFlag"},
	}
	return validateBindings(root, words)
}

func validateBindings(command *cobra.Command, words *text) error {
	if id, exists := command.Annotations["fathomry.short.id"]; exists {
		if _, err := words.render(id, "en"); err != nil {
			return err
		}
	}
	var bindingErr error
	check := func(flag *pflag.Flag) {
		if ids, exists := flag.Annotations["fathomry.usage.id"]; exists && bindingErr == nil {
			if len(ids) != 1 || ids[0] == "" {
				bindingErr = hostFailure(ErrDefinition)
				return
			}
			_, bindingErr = words.render(ids[0], "en")
		}
	}
	command.PersistentFlags().VisitAll(check)
	command.Flags().VisitAll(check)
	if bindingErr != nil {
		return bindingErr
	}
	for _, child := range command.Commands() {
		if err := validateBindings(child, words); err != nil {
			return err
		}
	}
	return words.failure()
}

func prepareCommand(command *cobra.Command, inheritedNames, inheritedShorts map[string]*pflag.Flag) error {
	if command.Args == nil || command.Run != nil || command.DisableFlagParsing ||
		command.RunE != nil && command.HasSubCommands() ||
		command.PreRun != nil || command.PreRunE != nil || command.PostRun != nil ||
		command.PostRunE != nil || command.PersistentPreRun != nil || command.PersistentPreRunE != nil ||
		command.PersistentPostRun != nil || command.PersistentPostRunE != nil ||
		command.FParseErrWhitelist.UnknownFlags || command.Deprecated != "" {
		return hostFailure(ErrDefinition)
	}
	flags := command.Flags()
	if flags.Parsed() || command.PersistentFlags().Parsed() ||
		flags.ParseErrorsWhitelist.UnknownFlags || flags.ParseErrorsAllowlist.UnknownFlags {
		return hostFailure(ErrDefinition)
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
		return hostFailure(ErrDefinition)
	}
	command.SilenceErrors, command.SilenceUsage = true, true
	command.DisableSuggestions = true
	command.CompletionOptions.DisableDefaultCmd = true
	siblings := map[string]bool{}
	for _, child := range command.Commands() {
		for _, name := range append([]string{child.Name()}, child.Aliases...) {
			if name == "" || strings.ContainsAny(name, " \t\r\n") || siblings[name] || reserved(name) {
				return hostFailure(ErrDefinition)
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
