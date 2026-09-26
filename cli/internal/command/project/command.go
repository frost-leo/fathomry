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

package project

import (
	"embed"
	"encoding/json"
	"errors"
	"io"

	"github.com/spf13/cobra"
)

//go:embed resources/*.json
var resources embed.FS

type prose struct{ english, chinese map[string]string }

func loadProse() prose {
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
	return prose{load("resources/en.json"), load("resources/zh-cn.json")}
}

func (words prose) get(command *cobra.Command, key string) string {
	if flag := command.Flag("lang"); flag != nil && flag.Value.String() == "zh-CN" {
		if translated := words.chinese[key]; translated != "" {
			return translated
		}
	}
	return words.english[key]
}

// New constructs invocation-local metadata. Source and filesystem inspection
// occur only when the host executes the validated command.
func New() *cobra.Command {
	words := loadProse()
	var input request
	command := &cobra.Command{
		Use: "new <directory>", Short: words.english["new"],
		Annotations: map[string]string{"fathomry.short.zh-CN": words.chinese["new"]},
	}
	command.Args = func(_ *cobra.Command, args []string) error {
		if len(args) != 1 {
			return errArguments
		}
		input.directory = args[0]
		return input.validate()
	}
	flags := command.Flags()
	flags.StringVar(&input.module, "module", "", words.english["module"])
	flags.StringVar(&input.source, "fathomry-source", "", words.english["source"])
	for name, key := range map[string]string{"module": "module", "fathomry-source": "source"} {
		flags.Lookup(name).Annotations = map[string][]string{"fathomry.usage.zh-CN": {words.chinese[key]}}
		if err := command.MarkFlagRequired(name); err != nil {
			panic(err)
		}
	}
	command.RunE = func(command *cobra.Command, _ []string) error {
		effect, err := create(command.Context(), input)
		if err != nil {
			key := "notStarted"
			if effect == partial {
				key = "partial"
			} else if effect == complete {
				key = "completeDelivery"
			}
			_, diagnosticErr := io.WriteString(command.ErrOrStderr(), words.get(command, key)+"\n")
			return errors.Join(err, diagnosticErr)
		}
		_, err = io.WriteString(command.OutOrStdout(), words.get(command, "created")+"\n")
		return err
	}
	return command
}
