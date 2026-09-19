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
	"io"
	"strings"

	"github.com/frost-leo/fathomry/i18n"
	"github.com/spf13/cobra"
)

type helpText struct {
	helpID    string
	summaryID string
	resources []i18n.Resource
}

// RootText names the root's external help resources. It is local presentation
// metadata, not a parser or command registration abstraction.
type RootText struct {
	HeaderID  string
	FooterID  string
	Resources []i18n.Resource
}

// Invocation is one CLI call's parser/presentation state. It is not reusable
// concurrently. Independent Run calls do not share flags, streams or locale.
type Invocation struct {
	ctx      context.Context
	out      io.Writer
	errOut   io.Writer
	locale   string
	helpErr  error
	help     map[*cobra.Command]helpText
	rootText RootText
}

// NewRoot creates the executable's bare command tree and generic help behavior.
// The caller adds only actual command families; no service is opened here.
func (call *Invocation) NewRoot(text RootText) *cobra.Command {
	call.rootText = text
	call.help = make(map[*cobra.Command]helpText)
	root := &cobra.Command{
		Use: "fathomry", SilenceErrors: true, SilenceUsage: true,
		DisableSuggestions: true, Args: cobra.NoArgs,
	}
	root.CompletionOptions.DisableDefaultCmd = true
	root.PersistentFlags().StringVar(&call.locale, "lang", "en", "")
	root.SetHelpFunc(func(command *cobra.Command, _ []string) {
		if command.HasAvailableSubCommands() && len(command.Flags().Args()) != 0 {
			call.helpErr = Usage()
			return
		}
		call.helpErr = call.Help(command)
	})
	helpCommand := &cobra.Command{
		Use: "help [command]", Args: cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			target, remaining, err := root.Find(args)
			if err != nil || target == nil || len(remaining) != 0 || target.Hidden {
				return Usage()
			}
			return call.Help(target)
		},
	}
	call.Add(root, helpCommand, "fathomry.cli.help.help", "fathomry.cli.command.help")
	root.SetHelpCommand(helpCommand)
	root.AddCommand(&cobra.Command{
		Use: "__complete", Aliases: []string{"__completeNoDesc"}, Hidden: true,
		RunE: func(*cobra.Command, []string) error { return Usage() },
	})
	return root
}

// Add registers a command in Cobra's actual tree and associates only its help
// resource IDs. It does not route commands or acquire their capabilities.
func (call *Invocation) Add(parent, command *cobra.Command, helpID, summaryID string, resources ...i18n.Resource) {
	parent.AddCommand(command)
	call.help[command] = helpText{helpID: helpID, summaryID: summaryID, resources: append([]i18n.Resource(nil), resources...)}
}

// Locale returns the current explicit language tag. Its default is en.
func (call *Invocation) Locale() string { return call.locale }

// ValidateLocale enforces the same tag grammar before text or JSON execution.
func (call *Invocation) ValidateLocale() error {
	if !validLocale(call.locale) {
		return Usage()
	}
	return nil
}

// Catalog prepares only the selected command's human resources on demand.
// JSON success does not need to call this method or inspect other commands.
func (call *Invocation) Catalog(selected ...i18n.Resource) (*i18n.Catalog, error) {
	resources := append(Resources(), selected...)
	return i18n.Prepare(resources)
}

// Help renders checked resource-owned help for the actual resolved command.
func (call *Invocation) Help(command *cobra.Command) error {
	if command.Hidden {
		return Usage()
	}
	if err := call.ValidateLocale(); err != nil {
		return err
	}
	if err := call.CheckContext(); err != nil {
		return err
	}
	var value string
	var err error
	if command.Parent() == nil {
		catalog, prepareErr := call.Catalog(call.rootText.Resources...)
		if prepareErr != nil {
			return Render(prepareErr)
		}
		value, err = call.rootHelp(catalog, command)
	} else if info, ok := call.help[command]; ok {
		catalog, prepareErr := call.Catalog(info.resources...)
		if prepareErr != nil {
			return Render(prepareErr)
		}
		var result i18n.Result
		result, err = catalog.Render(call.locale, info.helpID, nil)
		if err == nil {
			value = result.Text
			if command.HasAvailableSubCommands() {
				value, err = call.groupHelp(catalog, command, value)
			}
			value += "\n"
		}
	} else {
		return Usage()
	}
	if err != nil {
		return commandError{status: 1, code: renderCode, cause: err}
	}
	return call.WriteFinite(value, 64<<10)
}

func (call *Invocation) rootHelp(catalog *i18n.Catalog, root *cobra.Command) (string, error) {
	header, err := catalog.Render(call.locale, call.rootText.HeaderID, nil)
	if err != nil {
		return "", err
	}
	var text strings.Builder
	text.WriteString(header.Text)
	for _, command := range root.Commands() {
		if command.Hidden {
			continue
		}
		summary, err := call.summary(command)
		if err != nil {
			return "", err
		}
		text.WriteString("\n  ")
		text.WriteString(command.Name())
		text.WriteString("    ")
		text.WriteString(summary)
	}
	footer, err := catalog.Render(call.locale, call.rootText.FooterID, nil)
	if err != nil {
		return "", err
	}
	text.WriteString("\n")
	text.WriteString(footer.Text)
	text.WriteString("\n")
	return text.String(), nil
}

func (call *Invocation) groupHelp(catalog *i18n.Catalog, group *cobra.Command, intro string) (string, error) {
	heading, err := catalog.Render(call.locale, "fathomry.cli.help.commands_heading", nil)
	if err != nil {
		return "", err
	}
	var text strings.Builder
	text.WriteString(intro)
	text.WriteString("\n\n")
	text.WriteString(heading.Text)
	for _, command := range group.Commands() {
		if command.Hidden {
			continue
		}
		summary, err := call.summary(command)
		if err != nil {
			return "", err
		}
		text.WriteString("\n  ")
		text.WriteString(command.Name())
		text.WriteString("    ")
		text.WriteString(summary)
	}
	return text.String(), nil
}

func (call *Invocation) summary(command *cobra.Command) (string, error) {
	info, ok := call.help[command]
	if !ok {
		return "", commandError{status: 1, code: renderCode}
	}
	catalog, err := call.Catalog(info.resources...)
	if err != nil {
		return "", err
	}
	result, err := catalog.Render(call.locale, info.summaryID, nil)
	return result.Text, err
}

func (call *Invocation) report(err error) int {
	classified, ok := err.(commandError)
	if !ok {
		classified = commandError{status: 1, code: renderCode, cause: err}
	}
	if call.errOut == nil {
		return classified.status
	}
	locale := call.locale
	if !validLocale(locale) {
		locale = "en"
	}
	message := string(classified.code)
	catalog, catalogErr := call.Catalog(classified.resources...)
	if catalogErr == nil {
		if result, renderErr := catalog.Render(locale, string(classified.code), nil); renderErr == nil {
			message = result.Text
		} else if fallback, fallbackErr := catalog.Render(locale, string(execCode), nil); fallbackErr == nil {
			message = fallback.Text
		}
		if classified.status == 2 {
			if hint, hintErr := catalog.Render(locale, "fathomry.cli.help.hint", nil); hintErr == nil {
				message += "\n" + hint.Text
			}
		}
	}
	if len(message)+1 <= 4<<10 {
		_ = writeChecked(call.errOut, message+"\n")
	}
	return classified.status
}
