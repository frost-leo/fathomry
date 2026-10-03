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
	"path/filepath"
	"strings"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"golang.org/x/mod/module"
)

const frameworkModule = "github.com/frost-leo/fathomry"
const minimumGo = "1.27.0"

type request struct {
	destination, name, module string
	mode, provider, encoding  string
	frameworkSource           string
}
type plan struct {
	destination, name, module string
	mode, provider, encoding  string
	dependency                dependency
	files                     []projectFile
}
type projectFile struct {
	name    string
	content []byte
}

func prepare(input request) (plan, error) {
	if len(input.destination) == 0 || len(input.destination) > 4096 || len(input.module) > 512 || input.module == frameworkModule || module.CheckPath(input.module) != nil {
		return plan{}, command.Fail(command.ErrUsage)
	}
	for _, component := range strings.Split(input.module, "/") {
		if component == "vendor" {
			return plan{}, command.Fail(command.ErrUsage)
		}
	}
	if input.mode != "local" && input.mode != "remote" || input.encoding != "yaml" && input.encoding != "toml" {
		return plan{}, command.Fail(command.ErrUsage)
	}
	expected := "viper"
	if input.mode == "remote" {
		expected = "nacos"
	}
	if input.provider != expected {
		return plan{}, command.Fail(command.ErrUsage)
	}
	destination, err := filepath.Abs(input.destination)
	if err != nil {
		return plan{}, command.Fail(command.ErrUsage, err)
	}
	if input.name == "" {
		input.name = filepath.Base(destination)
	}
	if !validProjectName(input.name) {
		return plan{}, command.Fail(command.ErrUsage)
	}
	selected, err := selectDependency(input.frameworkSource)
	if err != nil {
		return plan{}, err
	}
	result := plan{destination: destination, name: input.name, module: input.module, mode: input.mode, provider: input.provider, encoding: input.encoding, dependency: selected}
	result.files, err = render(result)
	return result, err
}

func validProjectName(name string) bool {
	if len(name) == 0 || len(name) > 64 || name[0] < 'a' || name[0] > 'z' || name == "vendor" || name == "testdata" {
		return false
	}
	for _, char := range name {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') {
			return false
		}
	}
	for _, reserved := range []string{"con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"} {
		if strings.EqualFold(name, reserved) {
			return false
		}
	}
	return true
}
