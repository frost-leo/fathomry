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

package minio

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/objectstore/minio/v7"
)

// Dependencies borrows caller-owned admission, independent evidence and optional
// diagnostics. Closing a source never closes these shared owners.
type Dependencies struct {
	Runtime  *adapters.Runtime
	Evidence *adapters.Inbox[Result]
	Observer *adapters.Observer
}

// Settings is strict-loadable data, not a native options alias. Strings are
// frozen at Open; no ambient credentials, endpoint discovery or refresh is used.
// Durations are nanoseconds. Zero selects native defaults except QueuedCalls
// disables queueing and capability booleans deny authority. Version zero means 1.
// Defaults: 4 active, 64 MiB transfer, 8 MiB read, 5 MiB parts, 64 parts,
// 128 entries, 1 MiB control responses, 128 requests, 30s operation,
// 5s cleanup, 5m session and 15m maximum presign expiry. Prefix is literal.
type Settings struct {
	Name             string        `json:"name"`
	Version          uint32        `json:"version"`
	Endpoint         string        `json:"endpoint"`
	Plaintext        bool          `json:"plaintext"`
	RootCAPEM        string        `json:"root_ca_pem"`
	Region           string        `json:"region"`
	Bucket           string        `json:"bucket"`
	Prefix           string        `json:"prefix"`
	AccessKey        string        `json:"access_key"`
	SecretKey        string        `json:"secret_key"`
	SessionToken     string        `json:"session_token"`
	Writes           bool          `json:"writes"`
	Versions         bool          `json:"versions"`
	Tags             bool          `json:"tags"`
	PresignGET       bool          `json:"presign_get"`
	PresignHEAD      bool          `json:"presign_head"`
	PresignPUT       bool          `json:"presign_put"`
	MaxPresignExpiry time.Duration `json:"max_presign_expiry_ns"`
	SessionTimeout   time.Duration `json:"session_timeout_ns"`
	MaxActive        int           `json:"max_active"`
	QueuedCalls      int           `json:"queued_calls"`
	MaxTransferBytes int64         `json:"max_transfer_bytes"`
	MaxReadBytes     int           `json:"max_read_bytes"`
	PartBytes        int           `json:"part_bytes"`
	MaxParts         int           `json:"max_parts"`
	MaxEntries       int           `json:"max_entries"`
	MaxResponseBytes int64         `json:"max_response_bytes"`
	MaxRequests      int           `json:"max_requests"`
	Timeout          time.Duration `json:"timeout_ns"`
	CleanupTimeout   time.Duration `json:"cleanup_timeout_ns"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{
		Name:             value.Name,
		Version:          value.Version,
		Endpoint:         value.Endpoint,
		Plaintext:        value.Plaintext,
		RootCAPEM:        value.RootCAPEM,
		Region:           value.Region,
		Bucket:           value.Bucket,
		Prefix:           value.Prefix,
		AccessKey:        value.AccessKey,
		SecretKey:        value.SecretKey,
		SessionToken:     value.SessionToken,
		Writes:           value.Writes,
		Versions:         value.Versions,
		Tags:             value.Tags,
		PresignGET:       value.PresignGET,
		PresignHEAD:      value.PresignHEAD,
		PresignPUT:       value.PresignPUT,
		MaxPresignExpiry: value.MaxPresignExpiry,
		SessionTimeout:   value.SessionTimeout,
		MaxActive:        value.MaxActive,
		QueuedCalls:      value.QueuedCalls,
		MaxTransferBytes: value.MaxTransferBytes,
		MaxReadBytes:     value.MaxReadBytes,
		PartBytes:        value.PartBytes,
		MaxParts:         value.MaxParts,
		MaxEntries:       value.MaxEntries,
		MaxResponseBytes: value.MaxResponseBytes,
		MaxRequests:      value.MaxRequests,
		Timeout:          value.Timeout,
		CleanupTimeout:   value.CleanupTimeout,
	}
}

// Validate performs the same pure preparation as Open, without readiness I/O.
func Validate(value Settings) error {
	_, err := native.Select(options(value))
	return translate(err, "validate")
}

// Recommend supplies ceilings for one source generation at its native capacity.
// For Follow overlap, request ForGenerations before constructing shared owners.
func Recommend(value Settings) (objectstore.Policy, error) {
	if err := Validate(value); err != nil {
		return objectstore.Policy{}, err
	}
	nativeBudget := native.BudgetV1(options(value))
	budget := objectstore.Budget{WorkBytes: nativeBudget.WorkBytes + 3*nativeBudget.EvidenceBytes + 256<<10, EvidenceBytes: nativeBudget.EvidenceBytes + 64<<10}
	capacity := 1 + 2*nativeBudget.Active + nativeBudget.Queued
	return objectstore.Policy{Budget: budget, Runtime: adapters.Options{
		MaxActive: 1 + nativeBudget.Active, MaxQueued: nativeBudget.Queued,
		MaxWorkBytes:   sourceWorkBytes + int64(nativeBudget.Active)*budget.WorkBytes,
		MaxQueuedBytes: int64(nativeBudget.Queued) * budget.WorkBytes, MaxTasks: 2, MaxDepth: 2, MaxHolds: 4,
	}, Evidence: adapters.EvidenceOptions{Capacity: capacity, MaxBytes: sourceEvidenceBytes + int64(capacity-1)*budget.EvidenceBytes}}, nil
}
