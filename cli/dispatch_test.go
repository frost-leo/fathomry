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
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func TestDispatchInheritedValueForms(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		value string
	}{
		{"separate", []string{"-v", "-c", "alpha", "beta"}, "alpha"},
		{"cluster", []string{"-vc", "alpha", "beta"}, "alpha"},
		{"cluster-equals", []string{"-vc=alpha", "beta"}, "alpha"},
		{"cluster-attached", []string{"-vcalpha", "beta"}, "alpha"},
		{"command-first", []string{"beta", "-vc", "alpha"}, "alpha"},
		{"long", []string{"--config", "alpha", "beta"}, "alpha"},
		{"long-equals", []string{"--config=alpha", "beta"}, "alpha"},
		{"terminator-value", []string{"-vc", "--", "beta"}, "--"},
		{"help-value", []string{"-vc", "--help", "beta"}, "--help"},
		{"group-value", []string{"-vc", "family", "family", "beta"}, "family"},
		{"between-levels", []string{"family", "-vc", "alpha", "beta"}, "alpha"},
		{"nested-cluster", []string{"-vc", "alpha", "family", "beta"}, "alpha"},
	} {
		t.Run(test.name, func(t *testing.T) {
			effect := filepath.Join(t.TempDir(), "effect")
			var value string
			status, err, _, _ := capture(context.Background(), test.args, func(words *text) *cobra.Command {
				root := commands(words)
				root.PersistentFlags().BoolP("verbose", "v", false, "")
				root.PersistentFlags().StringVarP(&value, "config", "c", "", "")
				family := &cobra.Command{Use: "family", Args: cobra.NoArgs}
				addLeaves := func(parent *cobra.Command) {
					for _, name := range []string{"alpha", "beta"} {
						parent.AddCommand(&cobra.Command{Use: name, Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error {
							file, err := os.OpenFile(effect, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
							if err != nil {
								return err
							}
							_, err = io.WriteString(file, command.Name()+":"+value+"\n")
							return errors.Join(err, file.Close())
						}})
					}
				}
				addLeaves(root)
				addLeaves(family)
				root.AddCommand(family)
				return root
			})
			recorded, readErr := os.ReadFile(effect)
			if status != 0 || err != nil || readErr != nil || string(recorded) != "beta:"+test.value+"\n" {
				t.Fatalf("status=%d error=%v effect=%q read_error=%v", status, err, recorded, readErr)
			}
		})
	}
}

func TestDispatchPrefixHelpTerminatorsAndValidation(t *testing.T) {
	for _, test := range []struct {
		args    []string
		status  int
		invoked bool
		zh      bool
	}{
		{[]string{"--help", "family", "beta"}, 0, false, false},
		{[]string{"family", "--help", "beta"}, 0, false, false},
		{[]string{"-vh", "family", "beta"}, 0, false, false},
		{[]string{"--help", "family", "missing"}, 2, false, false},
		{[]string{"--", "family", "beta"}, 2, false, false},
		{[]string{"family", "--", "beta"}, 2, false, false},
		{[]string{"--lang=zh-CN", "family", "missing"}, 2, false, true},
		{[]string{"family", "missing", "--lang=zh-CN"}, 2, false, true},
		{[]string{"--bad", "--lang=zh-CN", "family", "beta"}, 2, false, false},
		{[]string{"--lang=zh-CN", "family", "--bad"}, 2, false, true},
		{[]string{"--left", "family", "beta", "--token=x"}, 2, false, false},
		{[]string{"--left", "family", "--right", "beta", "--token=x"}, 0, true, false},
		{[]string{"--token=x", "family", "beta"}, 0, true, false},
		{[]string{"--help", "family", "beta", "--help=false", "--token=x"}, 0, true, false},
		{[]string{"--lang=zh-CN", "family", "--lang=en", "beta", "--help"}, 0, false, false},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			invoked := false
			status, err, out, diagnostic := capture(context.Background(), test.args, func(words *text) *cobra.Command {
				root := commands(words)
				root.PersistentFlags().BoolP("verbose", "v", false, "")
				root.PersistentFlags().String("token", "", "")
				if err := root.MarkPersistentFlagRequired("token"); err != nil {
					t.Fatal(err)
				}
				root.PersistentFlags().Bool("left", false, "")
				root.PersistentFlags().Bool("right", false, "")
				root.MarkFlagsRequiredTogether("left", "right")
				family := &cobra.Command{Use: "family", Args: cobra.NoArgs}
				family.AddCommand(&cobra.Command{Use: "beta", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { invoked = true; return nil }})
				root.AddCommand(family)
				return root
			})
			if status != test.status || invoked != test.invoked {
				t.Fatalf("status=%d error=%v invoked=%v stdout=%q stderr=%q", status, err, invoked, out, diagnostic)
			}
			words := newText()
			if test.zh {
				if !strings.Contains(diagnostic, words.chinese["invalid"]) {
					t.Fatalf("wrong language: %q", diagnostic)
				}
			} else if strings.Contains(diagnostic, words.chinese["invalid"]) || strings.Contains(out, words.chinese["usage"]) {
				t.Fatalf("wrong language: %q %q", out, diagnostic)
			}
		})
	}
}

func TestDispatchParsesEachOccurrenceOnce(t *testing.T) {
	var count int
	var tags []string
	var value string
	var actualArgs []string
	var dash int
	status, err, _, _ := capture(context.Background(), []string{"-vvt", "first", "--mode=family", "family", "-vt", "second", "beta", "-vt", "third", "one", "--", "--lang=zh-CN"}, func(words *text) *cobra.Command {
		root := commands(words)
		root.PersistentFlags().CountVarP(&count, "verbose", "v", "")
		root.PersistentFlags().StringSliceVarP(&tags, "tag", "t", nil, "")
		root.PersistentFlags().StringVar(&value, "mode", "", "")
		root.PersistentFlags().Lookup("mode").NoOptDefVal = "default"
		family := &cobra.Command{Use: "family", Args: cobra.NoArgs}
		family.AddCommand(&cobra.Command{Use: "beta", Args: cobra.ArbitraryArgs, RunE: func(command *cobra.Command, args []string) error {
			actualArgs = append([]string{}, args...)
			dash = command.ArgsLenAtDash()
			if !command.Flags().Changed("mode") || !command.Flags().Changed("verbose") {
				t.Error("inherited Changed state lost")
			}
			return nil
		}})
		root.AddCommand(family)
		return root
	})
	if status != 0 || err != nil || count != 4 || !reflect.DeepEqual(tags, []string{"first", "second", "third"}) || value != "family" || dash != 1 || !reflect.DeepEqual(actualArgs, []string{"one", "--lang=zh-CN"}) {
		t.Fatalf("status=%d error=%v count=%d tags=%q value=%q dash=%d args=%q", status, err, count, tags, value, dash, actualArgs)
	}
}

func TestDispatchNormalizationUsesCurrentNode(t *testing.T) {
	for _, test := range []struct {
		args            []string
		status          int
		selected, value string
		verbose         bool
	}{
		{[]string{"--option", "alpha", "beta"}, 2, "", "", true},
		{[]string{"--option", "beta"}, 0, "beta", "", true},
		{[]string{"beta", "--option", "alpha"}, 0, "beta", "alpha", false},
	} {
		var selected, value string
		var verbose bool
		status, err, _, _ := capture(context.Background(), test.args, func(words *text) *cobra.Command {
			root := commands(words)
			root.Flags().SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
				if name == "option" {
					return "verbose"
				}
				return pflag.NormalizedName(name)
			})
			root.PersistentFlags().BoolVar(&verbose, "verbose", false, "")
			root.PersistentFlags().StringVar(&value, "config", "", "")
			for _, name := range []string{"alpha", "beta"} {
				leaf := &cobra.Command{Use: name, Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error { selected = command.Name(); return nil }}
				leaf.Flags().SetNormalizeFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
					if name == "option" {
						return "config"
					}
					return pflag.NormalizedName(name)
				})
				root.AddCommand(leaf)
			}
			return root
		})
		if status != test.status || selected != test.selected || value != test.value || verbose != test.verbose {
			t.Fatalf("args=%q status=%d error=%v selected=%q value=%q verbose=%v", test.args, status, err, selected, value, verbose)
		}
	}
}

func TestDispatchOptionalAndLeafLocalValues(t *testing.T) {
	for _, test := range []struct {
		args        []string
		status      int
		mode, value string
	}{
		{[]string{"-vm", "beta", "-c", "alpha"}, 0, "auto", "alpha"},
		{[]string{"-vm=alpha", "beta", "-c", "beta"}, 0, "alpha", "beta"},
		{[]string{"beta", "-vc", "alpha"}, 0, "", "alpha"},
		{[]string{"-c", "alpha", "beta"}, 2, "", ""},
		{[]string{"-vc", "alpha", "beta"}, 2, "", ""},
	} {
		var mode, value string
		invoked := false
		status, err, _, _ := capture(context.Background(), test.args, func(words *text) *cobra.Command {
			root := commands(words)
			root.PersistentFlags().BoolP("verbose", "v", false, "")
			root.PersistentFlags().StringVarP(&mode, "mode", "m", "", "")
			root.PersistentFlags().Lookup("mode").NoOptDefVal = "auto"
			leaf := &cobra.Command{Use: "beta", Args: cobra.NoArgs, RunE: func(*cobra.Command, []string) error { invoked = true; return nil }}
			leaf.Flags().StringVarP(&value, "config", "c", "", "")
			root.AddCommand(leaf)
			return root
		})
		if status != test.status || invoked != (test.status == 0) || mode != test.mode || value != test.value {
			t.Fatalf("args=%q status=%d error=%v invoked=%v mode=%q value=%q", test.args, status, err, invoked, mode, value)
		}
	}
}
