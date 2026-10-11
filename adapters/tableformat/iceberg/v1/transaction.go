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

func (s *Session) CommitTransaction(transactions ...*Transaction) error {
	if err := s.valid(); err != nil {
		return err
	}
	inputs := make([]*native.ManagedTransaction, len(transactions))
	for i, t := range transactions {
		if t == nil {
			return fail(ErrInput, "transaction")
		}
		inputs[i] = t.native
	}
	return translate(s.native.CommitTransaction(inputs...), "commit-transaction")
}
func (s *Session) CommitTableChanges(commits []table.TableCommit) error {
	if err := s.valid(); err != nil {
		return err
	}
	return translate(s.native.CommitTableChanges(commits), "commit-table-changes")
}
func (s *Session) CommitTransactionAndReload(transactions ...*Transaction) ([]*Table, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	inputs := make([]*native.ManagedTransaction, len(transactions))
	for i, t := range transactions {
		if t == nil {
			return nil, fail(ErrInput, "transaction")
		}
		inputs[i] = t.native
	}
	values, err := s.native.CommitTransactionAndReload(inputs...)
	result := make([]*Table, len(values))
	for i, v := range values {
		result[i] = wrapTable(v)
	}
	return result, translate(err, "commit-transaction-reload")
}
