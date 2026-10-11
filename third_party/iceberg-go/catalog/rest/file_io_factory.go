// fathomry
// Copyright (C) 2026  Frost Leo
// SPDX-License-Identifier: GPL-3.0-or-later
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package rest

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"reflect"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// FileIOFactory constructs a Catalog-local FileIO without consulting the global
// IO registry. Properties are a private copy of the effective Catalog, table,
// table-response and applicable vended-credential configuration. Location is an
// untrusted table metadata or object URI; the factory must validate its canonical
// form, allowed roots and credential audience before constructing any client.
//
// The factory must be safe for concurrent use. The caller owns all resources it
// constructs, including table FileIOs replaced by credential refresh. Plan-scoped
// FileIO caches may call Close on returned values, so a value that shares a
// physical owner must expose only independently closeable leases. Credentials
// are cached and refreshed independently; the factory is invoked with each
// acquisition's context rather than reusing a FileIO bound to an older context.
//
// A factory error is terminal for that construction; the Catalog never falls
// back to the global registry. Returning nil without an error is invalid.
type FileIOFactory func(context.Context, string, iceberg.Properties) (iceio.IO, error)

// TableValidator validates a newly loaded table before returning it, including
// refreshes performed inside commit retries. It may use the table's configured
// FileIO, but must not mutate it or extend its I/O lifetime. Errors do not trigger
// fallback or undo a preceding Catalog mutation.
type TableValidator func(context.Context, *table.Table) error

// WithTableValidator installs optional source-local validation on every table
// construction from a Catalog response. Pure native construction is unchanged
// when the callback is nil.
func WithTableValidator(validator TableValidator) Option {
	return func(options *options) { options.tableValidator = validator }
}

// WithFileIOFactory selects source-local FileIO construction for normal table
// loads, credential renewal and all prefix-scoped remote-plan IO. A nil factory
// preserves the upstream registry-based behavior.
func WithFileIOFactory(factory FileIOFactory) Option {
	return func(options *options) { options.fileIOFactory = factory }
}

// StorageCredentialValidator checks every vended credential before selection.
// Location is the metadata URI for table credentials and high-level planning,
// or empty for low-level plan responses that contain no metadata URI. Credentials
// and their configuration maps are private copies. Validation must inspect all
// entries, including entries whose prefixes do not match Location. The callback
// may reject unsolicited credentials, invalid prefixes, expiry or audience.
type StorageCredentialValidator func(context.Context, string, []StorageCredential) error

// WithStorageCredentialValidator installs an explicit source-local policy for
// vended credentials. It never selects credentials or performs IO by itself.
func WithStorageCredentialValidator(validator StorageCredentialValidator) Option {
	return func(options *options) { options.storageCredentialValidator = validator }
}

// WithLogger borrows a source-local logger for Catalog diagnostics and optional
// asynchronous REST metrics. The owner must retain it until Catalog.Close joins
// the dispatcher. Nil preserves the native default logger.
func WithLogger(logger *slog.Logger) Option {
	return func(options *options) { options.logger = logger }
}

func (r *Catalog) validateStorageCredentials(ctx context.Context, location string, credentials []StorageCredential) error {
	if r.storageCredentialValidator == nil || len(credentials) == 0 {
		return nil
	}
	return r.storageCredentialValidator(ctx, location, cloneStorageCredentials(credentials))
}

func cloneStorageCredentials(credentials []StorageCredential) []StorageCredential {
	result := make([]StorageCredential, len(credentials))
	for index, credential := range credentials {
		result[index] = StorageCredential{Prefix: credential.Prefix, Config: maps.Clone(credential.Config)}
	}
	return result
}

func (r *Catalog) log() *slog.Logger {
	if r.logger != nil {
		return r.logger
	}
	return slog.Default()
}

func loadFileIO(ctx context.Context, factory FileIOFactory, location string, props iceberg.Properties) (iceio.IO, error) {
	if err := ctx.Err(); err != nil {
		return nil, context.Cause(ctx)
	}
	if factory == nil {
		return iceio.LoadFS(ctx, props, location)
	}
	filesystem, err := factory(ctx, location, maps.Clone(props))
	if err != nil {
		return nil, err
	}
	if filesystem == nil || nilFileIO(filesystem) {
		return nil, fmt.Errorf("%w: FileIO factory returned nil without an error", iceberg.ErrInvalidArgument)
	}
	return filesystem, nil
}

func nilFileIO(filesystem iceio.IO) bool {
	value := reflect.ValueOf(filesystem)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
