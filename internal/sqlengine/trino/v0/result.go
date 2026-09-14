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

package trino

import "slices"

// Effect describes mutation evidence, independently of local or protocol completion.
type Effect uint8

const (
	// NotSubmitted means no statement RoundTrip entered the owned HTTP transport.
	NotSubmitted Effect = iota
	// Unknown means a mutation was attempted without a valid successful terminal reply.
	Unknown
	// Acknowledged means a successful terminal protocol reply was observed, not
	// independently reconciled durability, per-row success or multi-statement atomicity.
	Acknowledged
	// ReadOnly means the selected statement family was read-only, not an SQL sandbox.
	ReadOnly
)

// Column holds private result metadata. Type is Trino's textual type; Signature
// is its direct-protocol JSON signature. Access is intentional data disclosure.
type Column struct {
	private
	Name      string
	Type      string
	Signature []byte
}

// Result is immutable after publication. Copy methods transfer independent storage.
// DataCopy returns a JSON array of rows: numeric tokens stay exact, decimals and
// temporal values stay strings, binary stays base64, and nested/null shape is
// preserved. It does not apply Go local-timezone or float64 conversions.
type Result struct {
	private
	data *resultData
}
type resultData struct {
	effect             Effect
	queryID            string
	terminal           bool
	success            bool
	complete           bool
	cancelAttempted    bool
	cancelAcknowledged bool
	posts              uint64
	pages              uint64
	wireBytes          int64
	updateCount        *int64
	rows               int
	columns            []Column
	json               []byte
}

func (r Result) Effect() Effect {
	if r.data == nil {
		return NotSubmitted
	}
	return r.data.effect
}

// QueryID is the bounded coordinator ID, or empty when unobserved. It is private
// correlation data, not an idempotency token or durable resumption handle.
func (r Result) QueryID() string {
	if r.data == nil {
		return ""
	}
	return r.data.queryID
}

// Terminal means a valid identified protocol reply had no nextUri. It can be
// true alongside a server error or a local result-validation failure.
func (r Result) Terminal() bool { return r.data != nil && r.data.terminal }

// Succeeded means the terminal protocol reply contained no server error. It is
// independent of result fidelity, cleanup, connector durability and visibility.
func (r Result) Succeeded() bool { return r.data != nil && r.data.success }

// Complete requires successful terminal evidence and error-free native draining
// and result validation. Cleanup errors remain separate in the invocation outcome.
func (r Result) Complete() bool { return r.data != nil && r.data.complete }

// CancellationAttempted records entry into the explicitly budgeted DELETE path.
func (r Result) CancellationAttempted() bool { return r.data != nil && r.data.cancelAttempted }

// CancellationAcknowledged records HTTP 204, never remote termination or rollback.
func (r Result) CancellationAcknowledged() bool { return r.data != nil && r.data.cancelAcknowledged }

// Submissions counts statement POST entries into the owned transport (zero or
// one), not successful writes, server task attempts or byte-level transmissions.
func (r Result) Submissions() uint64 {
	if r.data == nil {
		return 0
	}
	return r.data.posts
}

// Pages counts fully buffered coordinator responses, including error responses
// and empty progress pages, but excluding the separate cleanup response.
func (r Result) Pages() uint64 {
	if r.data == nil {
		return 0
	}
	return r.data.pages
}

// WireBytes counts response-body bytes read, including the bounded cleanup body
// and a possible oversize detection byte. HTTP/TLS headers are not counted.
func (r Result) WireBytes() int64 {
	if r.data == nil {
		return 0
	}
	return r.data.wireBytes
}

// Rows counts retained query rows. Execute and Insert discard result rows while
// still validating their bounds; this count is never their affected-row count.
func (r Result) Rows() int {
	if r.data == nil {
		return 0
	}
	return r.data.rows
}

// UpdateCount distinguishes absent from aggregate zero. It is never a per-row receipt.
func (r Result) UpdateCount() (int64, bool) {
	if r.data == nil || r.data.updateCount == nil {
		return 0, false
	}
	return *r.data.updateCount, true
}

// DataCopy returns an independent finite JSON row array, possibly a validated
// prefix alongside failure. Complete must be checked before relying on exhaustion.
func (r Result) DataCopy() []byte {
	if r.data == nil {
		return nil
	}
	return slices.Clone(r.data.json)
}

// ColumnsCopy returns independent metadata and signature storage. Metadata can
// be partial after a rejected page; an empty slice does not establish no result.
func (r Result) ColumnsCopy() []Column {
	if r.data == nil {
		return nil
	}
	columns := slices.Clone(r.data.columns)
	for i := range columns {
		columns[i].Signature = slices.Clone(columns[i].Signature)
	}
	return columns
}
