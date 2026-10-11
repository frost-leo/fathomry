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

package iceberg

import (
	"github.com/apache/iceberg-go/table"
	native "github.com/frost-leo/fathomry/internal/tableformat/iceberg/v0"
)

type Transaction struct {
	private
	native *native.ManagedTransaction
}

func (t *Table) valid() error {
	if t == nil || t.native == nil {
		return fail(ErrState, "table")
	}
	return nil
}
func (t *Transaction) valid() error {
	if t == nil || t.native == nil {
		return fail(ErrState, "transaction")
	}
	return nil
}
func (t *Table) Identifier() []string {
	if t == nil {
		return nil
	}
	return t.native.Identifier()
}
func (t *Table) Metadata() (table.Metadata, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.Metadata()
	return v, translate(err, "metadata")
}

// MetadataLocation returns the exact originating metadata object for inspection
// or an explicitly authorized RegisterTable operation, without reloading.
func (t *Table) MetadataLocation() (string, error) {
	if err := t.valid(); err != nil {
		return "", err
	}
	location, err := t.native.MetadataLocation()
	return location, translate(err, "metadata-location")
}

func (t *Table) Refresh() (*Table, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.Refresh()
	return wrapTable(v), translate(err, "refresh")
}
func (t *Table) NewTransaction(branch string) (*Transaction, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.NewTransaction(branch)
	if v == nil {
		return nil, translate(err, "transaction")
	}
	return &Transaction{native: v}, translate(err, "transaction")
}
func (t *Transaction) StagedMetadata() (table.Metadata, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.StagedMetadata()
	return v, translate(err, "staged-metadata")
}
func (t *Transaction) TableCommit() (table.TableCommit, error) {
	if err := t.valid(); err != nil {
		return table.TableCommit{}, err
	}
	v, err := t.native.TableCommit()
	return v, translate(err, "table-commit")
}
func (t *Transaction) Commit() (*Table, error) {
	if err := t.valid(); err != nil {
		return nil, err
	}
	v, err := t.native.Commit()
	return wrapTable(v), translate(err, "commit")
}
