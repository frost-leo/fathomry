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

package create

import (
	"strings"

	"golang.org/x/mod/module"
)

const (
	frameworkModule  = "github.com/frost-leo/fathomry"
	frameworkVersion = "v0.0.0-20261028081050-b28e37d55e53"
	goVersion        = "1.27.0"
	maxRenderedBytes = 1 << 20
	maxReportBytes   = 16 << 10
)

type options struct {
	Name             string
	Module           string
	FrameworkVersion string
}

func validate(value options) error {
	if len(value.Name) == 0 || len(value.Name) > 63 ||
		value.Name[0] < 'a' || value.Name[0] > 'z' {
		return fail(ErrInput, "validate")
	}
	for _, char := range value.Name {
		if !(char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-') {
			return fail(ErrInput, "validate")
		}
	}
	if err := module.CheckImportPath(value.Name); err != nil {
		return fail(ErrInput, "validate", err)
	}
	if len(value.Module) == 0 || len(value.Module) > 1024 {
		return fail(ErrInput, "validate")
	}
	if err := module.CheckPath(value.Module); err != nil {
		return fail(ErrInput, "validate", err)
	}
	for _, reserved := range []string{
		frameworkModule, "github.com/spf13/cobra", "github.com/spf13/pflag",
		"github.com/inconshreveable/mousetrap", "golang.org/x/text",
	} {
		if value.Module == reserved {
			return fail(ErrInput, "module_identity")
		}
	}
	if len(value.FrameworkVersion) > 128 || value.FrameworkVersion != frameworkVersion {
		return fail(ErrInput, "framework_version")
	}
	return nil
}

func platform(goos string) error {
	if goos != "linux" {
		return fail(ErrPlatform, "platform")
	}
	return nil
}

func validRelative(name string) bool {
	if name == "" || strings.ContainsAny(name, "\\\x00") || strings.HasPrefix(name, "/") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}
