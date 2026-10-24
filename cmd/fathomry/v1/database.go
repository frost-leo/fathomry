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
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	createcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
)

// DatabaseConfiguration is a configuration-only in-process project handoff, not a durable
// document. Records are immutable released configuration evidence, not DB handles.
// Static explicitly authored settings may have no records. At most 128 records.
type DatabaseConfiguration struct {
	Dialect                 string
	Schema                  string
	PostgreSQL              *postgres.Settings
	MySQL                   *mysql.Settings
	Timeout, CleanupTimeout time.Duration
	Records                 []configuration.Record
}

// DatabaseResolver is an explicit lazy configuration-only project boundary. It must be
// finite/context-aware and return no live sources; nil is not ambient discovery.
type DatabaseResolver func(context.Context) (DatabaseConfiguration, error)

// resolveDatabase admits a project handoff without opening a database. Failed or
// incomplete configuration evidence prevents database ownership and mutation.
// A bounded handoff remains available on failure; excessive records are refused
// without copying them. A returned error never grants permission to open Settings.
func resolveDatabase(ctx context.Context, resolver DatabaseResolver) (DatabaseConfiguration, error) {
	if ctx == nil {
		return DatabaseConfiguration{}, databaseFailure(databasecmd.ErrInput, "resolve")
	}
	if resolver == nil {
		return DatabaseConfiguration{}, databaseFailure(databasecmd.ErrConfiguration, "resolve")
	}
	if ctx.Err() != nil {
		return DatabaseConfiguration{}, databaseFailure(databasecmd.ErrCanceled, "resolve", ctx.Err(), context.Cause(ctx))
	}
	value, err := resolver(ctx)
	if len(value.Records) > 128 {
		return DatabaseConfiguration{}, databaseFailure(databasecmd.ErrLimit, "resolve", err)
	}
	value = value.copy()
	if err != nil {
		return value, databaseFailure(databasecmd.ErrConfiguration, "resolve", err)
	}
	if err := checkDatabaseRecords(ctx, value); err != nil {
		return value, err
	}
	return normalizeDatabase(value)
}

// checkDatabaseRecords verifies the data-only handoff before any database ownership.
// Errors/cleanup in supplied configuration records are retained and refuse startup.
func checkDatabaseRecords(ctx context.Context, value DatabaseConfiguration) error {
	if ctx == nil {
		return databaseFailure(databasecmd.ErrInput, "configuration")
	}
	if ctx.Err() != nil {
		return databaseFailure(databasecmd.ErrCanceled, "configuration", ctx.Err(), context.Cause(ctx))
	}
	if len(value.Records) > 128 {
		return databaseFailure(databasecmd.ErrLimit, "configuration")
	}
	for _, record := range value.Records {
		if !record.Info().Released || !record.Info().Resolved || record.Err() != nil {
			return databaseFailure(databasecmd.ErrConfiguration, "configuration", record.Err())
		}
	}
	return nil
}

func (DatabaseConfiguration) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("cli.DatabaseConfiguration"))
}
func (*DatabaseConfiguration) LogValue() slog.Value {
	return slog.StringValue("cli.DatabaseConfiguration")
}
func (DatabaseConfiguration) MarshalJSON() ([]byte, error) {
	return nil, databaseFailure(databasecmd.ErrSerialization, "marshal")
}
func (*DatabaseConfiguration) UnmarshalJSON([]byte) error {
	return databaseFailure(databasecmd.ErrSerialization, "unmarshal")
}
func databaseFailure(code failure.Code, operation string, causes ...error) error {
	for _, definition := range databasecmd.Definitions() {
		if definition.Code == code {
			value, err := failure.New(definition, failure.Location{Operation: operation}, causes...)
			if err != nil {
				return err
			}
			return value
		}
	}
	return failure.ErrDefinition
}

func (value DatabaseConfiguration) copy() DatabaseConfiguration {
	if value.PostgreSQL != nil {
		selected := *value.PostgreSQL
		value.PostgreSQL = &selected
	}
	if value.MySQL != nil {
		selected := *value.MySQL
		value.MySQL = &selected
	}
	value.Records = slices.Clone(value.Records)
	return value
}

func normalizeDatabase(input DatabaseConfiguration) (DatabaseConfiguration, error) {
	value := input.copy()
	options, err := (databasecmd.Options{Timeout: value.Timeout, CleanupTimeout: value.CleanupTimeout}).Normalize()
	if err != nil {
		return value, databaseFailure(databasecmd.ErrInput, "configuration", err)
	}
	value.Timeout, value.CleanupTimeout = options.Timeout, options.CleanupTimeout
	switch value.Dialect {
	case "postgresql":
		if value.PostgreSQL == nil || value.MySQL != nil {
			return value, databaseFailure(databasecmd.ErrInput, "configuration")
		}
		if err = databasecmd.ValidateTarget(value.Dialect, value.Schema); err == nil {
			err = postgres.Validate(*value.PostgreSQL)
		}
	case "mysql":
		if value.MySQL == nil || value.PostgreSQL != nil || value.Schema != "" {
			return value, databaseFailure(databasecmd.ErrInput, "configuration")
		}
		if err = databasecmd.ValidateTarget(value.Dialect, value.MySQL.Database); err == nil {
			err = mysql.Validate(*value.MySQL)
		}
	default:
		err = databasecmd.ErrInput
	}
	if err != nil {
		return value, databaseFailure(databasecmd.ErrInput, "configuration", err)
	}
	return value, nil
}

type administration struct {
	User                string `json:"user" mapstructure:"user"`
	Password            string `json:"password" mapstructure:"password"`
	MaintenanceDatabase string `json:"maintenance_database" mapstructure:"maintenance_database"`
}

func validateAdministration(value administration) error {
	for index, field := range []string{value.User, value.Password, value.MaintenanceDatabase} {
		maximum := 256
		if index == 1 {
			maximum = 4096
		}
		if len(field) > maximum || !utf8.ValidString(field) || strings.ContainsRune(field, 0) || index == 0 && field == "" {
			return databaseFailure(databasecmd.ErrInput, "administration")
		}
	}
	return nil
}
func (administration) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("cli.administration"))
}
func (administration) LogValue() slog.Value { return slog.StringValue("cli.administration") }

func creationOptions(value DatabaseConfiguration) createcmd.CreateOptions {
	options := createcmd.CreateOptions{Dialect: value.Dialect, Schema: value.Schema, Timeout: value.Timeout, CleanupTimeout: value.CleanupTimeout}
	if value.PostgreSQL != nil {
		options.Database, options.Owner = value.PostgreSQL.Database, value.PostgreSQL.User
	}
	if value.MySQL != nil {
		options.Database = value.MySQL.Database
	}
	return options
}
func administrativeDatabase(target DatabaseConfiguration, admin administration) (DatabaseConfiguration, error) {
	if err := validateAdministration(admin); err != nil {
		return target, err
	}
	if err := createcmd.ValidateCreation(creationOptions(target)); err != nil {
		return target, err
	}
	value := target.copy()
	var err error
	if value.PostgreSQL != nil {
		if admin.MaintenanceDatabase == "" || admin.MaintenanceDatabase == target.PostgreSQL.Database {
			return value, databaseFailure(databasecmd.ErrInput, "administration")
		}
		value.PostgreSQL.Name = "control-admin"
		value.PostgreSQL.Database = admin.MaintenanceDatabase
		value.PostgreSQL.User, value.PostgreSQL.Password = admin.User, admin.Password
		err = postgres.Validate(*value.PostgreSQL)
	} else {
		if admin.MaintenanceDatabase != "" {
			return value, databaseFailure(databasecmd.ErrInput, "administration")
		}
		value.MySQL.Name = "control-admin"
		value.MySQL.Database = ""
		value.MySQL.User, value.MySQL.Password = admin.User, admin.Password
		err = mysql.Validate(*value.MySQL)
	}
	if err != nil {
		return value, databaseFailure(databasecmd.ErrInput, "administration", err)
	}
	return value, nil
}
