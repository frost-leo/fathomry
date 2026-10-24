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

package database

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestControlAdmission(t *testing.T) {
	value, err := (Options{}).Normalize()
	if err != nil || value.Timeout != 45*time.Second || value.CleanupTimeout != 5*time.Second {
		t.Fatal("control defaults changed", value, err)
	}
	for _, duration := range []time.Duration{time.Millisecond, time.Minute} {
		input := Options{duration, duration}
		if value, err := input.Normalize(); err != nil || value != input {
			t.Fatal("admitted duration changed", err)
		}
	}
	for _, duration := range []time.Duration{-1, time.Nanosecond, time.Minute + 1} {
		for _, input := range []Options{{Timeout: duration}, {CleanupTimeout: duration}} {
			if _, err := input.Normalize(); !errors.Is(err, ErrInput) {
				t.Fatal("unbounded duration admitted", input)
			}
		}
	}
	for _, dialect := range []string{"postgresql", "mysql"} {
		if err := ValidateTarget(dialect, "control"); err != nil {
			t.Fatal(err)
		}
		for _, target := range []string{"", "information_schema", "invalid\x00", "invalid\n"} {
			if !errors.Is(ValidateTarget(dialect, target), ErrInput) {
				t.Fatal("unsafe control target admitted")
			}
		}
	}
	for _, test := range [][2]string{{"postgresql", "$catalog_table$"}, {"postgresql", "pg_temp"}, {"postgresql", strings.Repeat("x", 64)}, {"mysql", "MYSQL"}, {"mysql", "a/b"}, {"mysql", "emoji😀"}, {"unknown", "control"}} {
		if !errors.Is(ValidateTarget(test[0], test[1]), ErrInput) {
			t.Fatal("unsupported target admitted", test[0])
		}
	}
}

func TestControlFailuresKeepTheirOperation(t *testing.T) {
	definitions := Definitions()
	if len(definitions) != 14 {
		t.Fatal("stable database identity inventory changed")
	}
	cause := errors.New("private-database-canary")
	for _, definition := range definitions {
		problem := Fail(definition.Code, "database.create", cause)
		core, known := failure.Inspect(problem)
		if !known || core.Diagnostic().Definition != definition || core.Diagnostic().Location.Operation != "database.create" || !errors.Is(problem, cause) || strings.Contains(problem.Error(), "private-database-canary") {
			t.Fatal("control occurrence lost identity, operation or private cause", problem)
		}
	}
	if !errors.Is(Fail(ErrDatabase, "schema.apply", context.Canceled), ErrCanceled) || !errors.Is(Fail(ErrDatabase, "schema.apply", adapters.ErrEvidence), ErrLimit) {
		t.Fatal("operation-level failure classification changed")
	}
	if _, err := i18n.Prepare(Components()...); err != nil {
		t.Fatal("control resources are incomplete", err)
	}
}

func TestDatabaseNamespaceDoesNotOwnLeaves(t *testing.T) {
	group := New(&command.Invocation{})
	if group.Name() != "database" || len(group.Commands()) != 0 {
		t.Fatal("common contract owner attached an executor")
	}
}
