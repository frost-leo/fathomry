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

// Package database owns common control-database admission/diagnostic contracts
// and the database command namespace. It creates no SQL clients or executors;
// the CLI entry attaches the fixed command leaves.
package database

import (
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/spf13/cobra"
)

// Options bounds control-operation work and subsequent cleanup waiting.
// Zero values select 45s/5s; explicit values admit 1ms..1min independently.
type Options struct{ Timeout, CleanupTimeout time.Duration }

// Normalize returns a detached, defaulted value without acquiring any resource.
func (options Options) Normalize() (Options, error) {
	if options.Timeout == 0 {
		options.Timeout = 45 * time.Second
	}
	if options.CleanupTimeout == 0 {
		options.CleanupTimeout = 5 * time.Second
	}
	if options.Timeout < time.Millisecond || options.Timeout > time.Minute || options.CleanupTimeout < time.Millisecond || options.CleanupTimeout > time.Minute {
		return options, Fail(ErrInput, "database.options")
	}
	return options, nil
}

// ValidateTarget admits a physical control namespace without loading assets or
// dialing. PostgreSQL also excludes the initial assets' dollar-quote delimiter.
func ValidateTarget(dialect, target string) error {
	if target == "" || !utf8.ValidString(target) || strings.ContainsFunc(target, unicode.IsControl) {
		return Fail(ErrInput, "database.target")
	}
	switch dialect {
	case "postgresql":
		if len(target) > 63 || strings.Contains(target, "$catalog_table$") || strings.HasPrefix(target, "pg_") || target == "information_schema" {
			return Fail(ErrInput, "database.target")
		}
	case "mysql":
		if utf8.RuneCountInString(target) > 64 || strings.ContainsFunc(target, func(char rune) bool { return char > 0xffff }) || strings.ContainsAny(target, "./\\") || strings.HasSuffix(target, " ") || slices.Contains([]string{"mysql", "sys", "performance_schema", "information_schema"}, strings.ToLower(target)) {
			return Fail(ErrInput, "database.target")
		}
	default:
		return Fail(ErrInput, "database.target")
	}
	return nil
}

// New declares the namespace; the CLI entry attaches its fixed leaf set.
func New(invocation *command.Invocation) *cobra.Command {
	return invocation.Group("database", "fathomry.database_commands.root")
}
