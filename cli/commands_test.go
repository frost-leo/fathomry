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
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func TestDefinitionCollisionsRefuseBeforeExecution(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*cobra.Command)
	}{
		{"duplicate-name", func(root *cobra.Command) {
			root.AddCommand(&cobra.Command{Use: "same", Args: cobra.NoArgs}, &cobra.Command{Use: "same", Args: cobra.NoArgs})
		}},
		{"alias-name", func(root *cobra.Command) {
			root.AddCommand(&cobra.Command{Use: "first", Aliases: []string{"second"}, Args: cobra.NoArgs}, &cobra.Command{Use: "second", Args: cobra.NoArgs})
		}},
		{"alias-alias", func(root *cobra.Command) {
			root.AddCommand(&cobra.Command{Use: "first", Aliases: []string{"a"}, Args: cobra.NoArgs}, &cobra.Command{Use: "second", Aliases: []string{"a"}, Args: cobra.NoArgs})
		}},
		{"reserved-name", func(root *cobra.Command) { root.AddCommand(&cobra.Command{Use: "__complete", Args: cobra.NoArgs}) }},
		{"reserved-alias", func(root *cobra.Command) {
			root.AddCommand(&cobra.Command{Use: "first", Aliases: []string{"__completeNoDesc"}, Args: cobra.NoArgs})
		}},
		{"reserved-help", func(root *cobra.Command) { root.AddCommand(&cobra.Command{Use: "help", Args: cobra.NoArgs}) }},
		{"help-flag", func(root *cobra.Command) { root.Flags().Bool("help", false, "") }},
		{"help-shorthand", func(root *cobra.Command) { root.Flags().BoolP("other", "h", false, "") }},
		{"language-shadow", func(root *cobra.Command) {
			leaf := &cobra.Command{Use: "leaf", Args: cobra.NoArgs}
			leaf.Flags().String("lang", "", "")
			root.AddCommand(leaf)
		}},
		{"persistent-local-name", func(root *cobra.Command) {
			root.PersistentFlags().Bool("same", false, "")
			root.Flags().Bool("same", false, "")
		}},
		{"inherited-shorthand", func(root *cobra.Command) {
			root.PersistentFlags().BoolP("parent", "p", false, "")
			leaf := &cobra.Command{Use: "leaf", Args: cobra.NoArgs}
			leaf.Flags().BoolP("child", "p", false, "")
			root.AddCommand(leaf)
		}},
		{"required-args-profile", func(root *cobra.Command) { root.AddCommand(&cobra.Command{Use: "leaf"}) }},
		{"routing-local-option", func(root *cobra.Command) { root.Flags().BoolP("config", "c", false, "") }},
		{"legacy-unknown-allowlist", func(root *cobra.Command) { root.Flags().ParseErrorsWhitelist.UnknownFlags = true }},
		{"unknown-allowlist", func(root *cobra.Command) { root.Flags().ParseErrorsAllowlist.UnknownFlags = true }},
		{"constructor-parsed", func(root *cobra.Command) { _ = root.Flags().Parse([]string{"--"}) }},
		{"constructor-parsed-persistent", func(root *cobra.Command) { _ = root.PersistentFlags().Parse(nil) }},
		{"runnable-root", func(root *cobra.Command) {
			root.RunE = func(*cobra.Command, []string) error { t.Fatal("root handler ran"); return nil }
		}},
		{"prehook", func(root *cobra.Command) { root.PreRun = func(*cobra.Command, []string) { t.Fatal("prehook ran") } }},
		{"deprecated", func(root *cobra.Command) {
			root.Flags().Bool("old", false, "")
			_ = root.Flags().MarkDeprecated("old", "UNSAFE")
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			status, err, _, _ := capture(context.Background(), nil, func(words *text) *cobra.Command { root := commands(words); test.mutate(root); return root })
			if status != 1 || !errors.Is(err, errDefinition) {
				t.Fatalf("%d %v", status, err)
			}
		})
	}
}

func TestRunnableGroupsRefusedBeforeHelpOrExecution(t *testing.T) {
	for _, args := range [][]string{{"family"}, {"family", "leaf"}, {"family", "missing"}, {"help", "family", "missing"}, {"family", "missing", "--help"}} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			invoked := false
			status, err, output, _ := capture(context.Background(), args, func(words *text) *cobra.Command {
				root := commands(words)
				handler := func(*cobra.Command, []string) error { invoked = true; return nil }
				family := &cobra.Command{Use: "family", Args: cobra.NoArgs, RunE: handler}
				family.AddCommand(&cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: handler})
				root.AddCommand(family)
				return root
			})
			if status != 1 || !errors.Is(err, errDefinition) || invoked || output != "" {
				t.Fatalf("status=%d error=%v invoked=%v output=%q", status, err, invoked, output)
			}
		})
	}
}

func normalizeFlagAlias(_ *pflag.FlagSet, name string) pflag.NormalizedName {
	return pflag.NormalizedName(strings.ReplaceAll(name, "_", "-"))
}

func TestIncompatibleFlagNormalizationRefused(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(root, leaf *cobra.Command)
	}{
		{"inherited-collision", func(root, leaf *cobra.Command) {
			root.PersistentFlags().String("auth_token", "root", "")
			leaf.Flags().SetNormalizeFunc(normalizeFlagAlias)
			leaf.Flags().String("auth-token", "leaf", "")
		}},
		{"inherited-rename", func(root, leaf *cobra.Command) {
			root.PersistentFlags().String("auth_token", "root", "")
			leaf.Flags().SetNormalizeFunc(normalizeFlagAlias)
		}},
		{"persistent-rename", func(root, leaf *cobra.Command) {
			leaf.PersistentFlags().String("auth_token", "leaf", "")
			leaf.Flags().SetNormalizeFunc(normalizeFlagAlias)
		}},
		{"reserved-help", func(root, leaf *cobra.Command) {
			leaf.Flags().SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
				if name == "help" {
					return "assistance"
				}
				return pflag.NormalizedName(name)
			})
		}},
		{"reserved-language", func(root, leaf *cobra.Command) {
			root.PersistentFlags().SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
				if name == "lang" {
					return "language"
				}
				return pflag.NormalizedName(name)
			})
		}},
		{"saved-global-rename", func(root, leaf *cobra.Command) {
			leaf.SetGlobalNormalizationFunc(normalizeFlagAlias)
			leaf.Flags().SetNormalizeFunc(nil)
			leaf.PersistentFlags().SetNormalizeFunc(nil)
			leaf.PersistentFlags().String("auth_token", "leaf", "")
		}},
		{"constructor-merge-corruption", func(root, leaf *cobra.Command) {
			root.PersistentFlags().String("auth_token", "root", "")
			leaf.Flags().SetNormalizeFunc(normalizeFlagAlias)
			leaf.MarkFlagsOneRequired("auth-token")
		}},
	} {
		for _, args := range [][]string{{"leaf", "--auth_token=user"}, {"leaf", "--auth-token=user"}, {"help", "leaf"}} {
			t.Run(test.name+"/"+strings.Join(args, " "), func(t *testing.T) {
				invoked := false
				status, err, output, _ := capture(context.Background(), args, func(words *text) *cobra.Command {
					root := commands(words)
					leaf := &cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { invoked = true; return nil }}
					root.AddCommand(leaf)
					test.mutate(root, leaf)
					return root
				})
				if status != 1 || !errors.Is(err, errDefinition) || invoked || output != "" {
					t.Fatalf("status=%d error=%v invoked=%v output=%q", status, err, invoked, output)
				}
			})
		}
	}
}

func TestCanonicalFlagAliasesRemainUsable(t *testing.T) {
	for _, global := range []bool{false, true} {
		for _, args := range [][]string{{"leaf", "--auth_token=user", "--left_side", "--right_side"}, {"help", "leaf"}} {
			root := commands(newText())
			root.PersistentFlags().String("auth-token", "root", "")
			leaf := &cobra.Command{Use: "leaf", Args: cobra.NoArgs}
			root.AddCommand(leaf)
			if global {
				root.SetGlobalNormalizationFunc(normalizeFlagAlias)
			} else {
				leaf.Flags().SetNormalizeFunc(normalizeFlagAlias)
			}
			leaf.PersistentFlags().Bool("left-side", false, "")
			leaf.Flags().Bool("right-side", false, "")
			leaf.MarkFlagsRequiredTogether("left-side", "right-side")
			invoked := false
			leaf.RunE = func(command *cobra.Command, _ []string) error {
				invoked = true
				value, err := command.Flags().GetString("auth-token")
				if err != nil || value != "user" {
					t.Fatalf("value=%q error=%v", value, err)
				}
				return nil
			}
			status, err, output, _ := capture(context.Background(), args, func(*text) *cobra.Command { return root })
			if status != 0 || err != nil || invoked != (args[0] == "leaf") {
				t.Fatalf("global=%v args=%q status=%d error=%v invoked=%v", global, args, status, err, invoked)
			}
			if args[0] == "help" && (strings.Count(output, "--auth-token") != 1 || strings.Contains(output, "--auth_token")) {
				t.Fatalf("inconsistent canonical help: %q", output)
			}
			flag := root.PersistentFlags().Lookup("auth-token")
			if flag == nil || flag.Name != "auth-token" {
				t.Fatal("parent flag identity changed")
			}
		}
	}
}

func TestNativeMergedPersistentFlagsAndRequiredGroups(t *testing.T) {
	construct := func(words *text) *cobra.Command {
		root := commands(words)
		leaf := &cobra.Command{Use: "leaf", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return nil }}
		leaf.PersistentFlags().Bool("first", false, "")
		leaf.Flags().Bool("second", false, "")
		leaf.MarkFlagsRequiredTogether("first", "second")
		choice := &cobra.Command{Use: "choice", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { return nil }}
		choice.Flags().Bool("first", false, "")
		choice.Flags().Bool("second", false, "")
		choice.MarkFlagsOneRequired("first", "second")
		root.AddCommand(leaf, choice)
		return root
	}
	for _, test := range []struct {
		args   []string
		status int
	}{
		{[]string{"leaf", "--help"}, 0}, {[]string{"leaf", "--first"}, 2}, {[]string{"leaf", "--first", "--second"}, 0},
		{[]string{"choice", "--help"}, 0}, {[]string{"choice"}, 2}, {[]string{"choice", "--first"}, 0},
	} {
		status, err, _, _ := capture(context.Background(), test.args, construct)
		if status != test.status {
			t.Fatalf("%v: %d %v", test.args, status, err)
		}
	}
}
