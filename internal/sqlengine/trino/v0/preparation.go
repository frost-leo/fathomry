/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package trino

import (
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/frost-leo/fathomry/internal/resource"
)

// BudgetV1 describes conservative byte reservations, not measured RSS. SourceBytes
// includes readiness's native work and result overlap; every concurrently owned
// source generation needs its own reservation. Evidence remains independently owned
// after work releases. PageBytes covers one transferred page plus detached metadata;
// retaining additional consumer copies is the consumer's separate responsibility.
type BudgetV1 struct {
	Active              int
	SourceBytes         int64
	WorkBytes           int64
	EvidenceBytes       int64
	ReaderWorkBytes     int64
	ReaderEvidenceBytes int64
	ReaderTerminalBytes int64
	PageBytes           int64
}

// Preparation owns validated effective settings without opening a connection.
// Options is an intentional sensitive copy. Other accessors contain no credentials.
type Preparation struct {
	private
	prepared resource.Prepared[settings]
	options  OptionsV1
	budget   BudgetV1
	limits   resource.Limits
}

// PrepareV1 defaults zero-valued typed options before strict layers are resolved.
// An explicit zero in a layer is not defaulted again and rejects positive bounds.
func PrepareV1(options OptionsV1, layers ...resource.Layer) (Preparation, error) {
	return prepareV1(options, defaults(options), layers)
}

// PrepareResolvedV1 validates an already resolved configuration without applying
// defaults again. It is intended for a public strict settings schema's handoff.
func PrepareResolvedV1(options OptionsV1) (Preparation, error) {
	return prepareV1(options, inputSettings(options), nil)
}

func prepareV1(options OptionsV1, initial settings, layers []resource.Layer) (Preparation, error) {
	if len(options.Name) <= 64 {
		options.Name = strings.Clone(options.Name)
	}
	version := options.Version
	if version == 0 {
		version = 1
	}
	// Raw text is a lower bound on encoded defaults. Bound copying without
	// rejecting bounded semantic defaults that a later layer can repair.
	texts := [...]string{initial.Endpoint, initial.User, initial.Password, initial.BearerToken, initial.RootCAPEM, initial.Catalog, initial.Schema}
	remaining := 1 << 20
	var defaultsError error
	for _, text := range texts {
		if len(text) > remaining {
			defaultsError = errors.New("source: defaults exceed size limit")
			// Preserve resource.Prepare's identity, format and UTF-8 precedence.
			initial.Endpoint, initial.User, initial.Password, initial.BearerToken = "", "", "", ""
			initial.RootCAPEM, initial.Catalog, initial.Schema = "", "", ""
			for _, original := range texts {
				if !utf8.ValidString(original) {
					initial.Endpoint = "\xff"
					break
				}
			}
			layers = nil
			break
		}
		remaining -= len(text)
	}
	var effective settings
	prepared, err := resource.Prepare(resource.Schema[settings]{Format: 1, Defaults: initial, Validate: func(value settings) error {
		if defaultsError != nil {
			return defaultsError
		}
		if err := validate(value); err != nil {
			return err
		}
		effective = value
		return nil
	}}, resource.Input{Identity: resource.Identity{Provider: ProviderID, Name: options.Name}, Format: version, Layers: layers})
	if err != nil {
		return Preparation{}, err
	}
	budget, err := effective.budget()
	if err != nil {
		return Preparation{}, err
	}
	resolved := optionsFromSettings(effective)
	resolved.Name = options.Name
	resolved.Version = version
	return Preparation{prepared: prepared, options: resolved, budget: budget, limits: resource.Limits{Active: effective.MaxActive, Bytes: int64(effective.MaxActive) * max(budget.WorkBytes, budget.ReaderWorkBytes), MaxLeases: 1}}, nil
}

func optionsFromSettings(s settings) OptionsV1 {
	return OptionsV1{Endpoint: s.Endpoint, User: s.User, Password: s.Password, BearerToken: s.BearerToken, RootCAPEM: s.RootCAPEM,
		Plaintext: s.Plaintext, Catalog: s.Catalog, Schema: s.Schema, Writes: s.Writes, Maintenance: s.Maintenance,
		MaxActive: s.MaxActive, MaxSQLBytes: s.MaxSQLBytes, MaxParameters: s.MaxParameters, MaxRows: s.MaxRows, MaxColumns: s.MaxColumns,
		MaxPageBytes: s.MaxPageBytes, MaxResultBytes: s.MaxResultBytes, MaxPages: s.MaxPages, MaxWireBytes: s.MaxWireBytes,
		Timeout: s.Timeout, CleanupTimeout: s.CleanupTimeout, MaxReadRows: s.MaxReadRows, MaxReadPages: s.MaxReadPages,
		MaxReadWireBytes: s.MaxReadWireBytes, ReadTimeout: s.ReadTimeout}
}

func (s settings) budget() (BudgetV1, error) {
	if err := validate(s); err != nil {
		return BudgetV1{}, err
	}
	configuration := int64(len(s.Endpoint) + len(s.User) + len(s.Password) + len(s.BearerToken) + len(s.RootCAPEM) + len(s.Catalog) + len(s.Schema))
	budget := BudgetV1{Active: s.MaxActive, WorkBytes: s.reservation(), EvidenceBytes: s.evidenceBytes(), ReaderWorkBytes: s.readerReservation(),
		ReaderEvidenceBytes: s.readerEvidenceBytes(), ReaderTerminalBytes: s.readerEvidenceBytes(), PageBytes: int64(s.MaxPageBytes) + s.metadataBytes()}
	source, ok := addBudget(budget.WorkBytes, budget.EvidenceBytes, 1<<20, 8*configuration)
	if !ok || int64(s.MaxActive) > math.MaxInt64/max(budget.WorkBytes, budget.ReaderWorkBytes) {
		return BudgetV1{}, failure(ErrLimit, "budget")
	}
	budget.SourceBytes = source
	return budget, nil
}

func addBudget(values ...int64) (int64, bool) {
	var sum int64
	for _, value := range values {
		if value < 0 || value > math.MaxInt64-sum {
			return 0, false
		}
		sum += value
	}
	return sum, true
}

func (p Preparation) Options() OptionsV1                { return p.options }
func (p Preparation) Limits() resource.Limits           { return p.limits }
func (p Preparation) Budget() BudgetV1                  { return p.budget }
func (p Preparation) Description() resource.Description { return p.prepared.Description() }

// Select binds this exact preparation to the existing readiness/acquisition path.
func (p Preparation) Select() resource.Selection[Source] { return selectPrepared(p.prepared) }
