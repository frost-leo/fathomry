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

import "slices"

// Effect concerns Catalog acknowledgement, not downstream visibility or durability.
type Effect uint8

const (
	NotSubmitted Effect = iota
	Unknown
	Acknowledged
	Rejected
)

// FileEffect retains a single attempted file upload. URI is explicit private data,
// not a diagnostic label. Acknowledgement does not establish a table commit.
type FileEffect struct {
	private
	URI    string
	Effect Effect
}

// Result is immutable after publication. Copy methods transfer independent storage.
// Metadata and IPC bytes are intentional data access, not safe diagnostics.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	effect              Effect
	staged              bool
	reloaded            bool
	snapshotID          int64
	committedSnapshotID int64
	rows                int64
	complete            bool
	ipc                 []byte
	metadata            []byte
	names               []string
	files               []FileEffect
}

func (r Result) Effect() Effect {
	if r.data == nil {
		return NotSubmitted
	}
	return r.data.effect
}
func (r Result) Staged() bool   { return r.data != nil && r.data.staged }
func (r Result) Reloaded() bool { return r.data != nil && r.data.reloaded }
func (r Result) SnapshotID() int64 {
	if r.data == nil {
		return 0
	}
	return r.data.snapshotID
}

// CommittedSnapshotID is the snapshot named by a valid commit reply. SnapshotID
// separately records the subsequent reload, which can observe a concurrent head.
func (r Result) CommittedSnapshotID() int64 {
	if r.data == nil {
		return 0
	}
	return r.data.committedSnapshotID
}

// Rows reports returned read rows or staged input rows. It is not an affected-row
// count: Delete reports zero here even when it removes rows from the table.
func (r Result) Rows() int64 {
	if r.data == nil {
		return 0
	}
	return r.data.rows
}
func (r Result) Complete() bool { return r.data != nil && r.data.complete }
func (r Result) IPCCopy() []byte {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.ipc)
}
func (r Result) MetadataCopy() []byte {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.metadata)
}
func (r Result) NamesCopy() []string {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.names)
}
func (r Result) FilesCopy() []FileEffect {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.files)
}
