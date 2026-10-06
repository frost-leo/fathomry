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

package kafka

import (
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	native "github.com/frost-leo/fathomry/internal/broker/franz/v1"
)

// Settings is strict-loadable configuration data, not a runtime or native option.
// Zero selects the documented native default, except zero queue/retries disables
// them. Durations are integer nanoseconds. Credential-bearing SASL requires TLS;
// credentials/trust are explicit, never discovered. Manual routing is default;
// keyed messages require Partition=-1. ConsumerGroup is distinct from OffsetGroup,
// and requires explicit InitialOffset/ResetOffset: error, earliest or latest.
// Direct formatting/logging is redacted; explicit configuration JSON is sensitive.
// Normalize optional nil *Settings to untyped nil before logging. The value
// LogValue method covers values and non-nil pointers, not a nil method wrapper.
type Settings struct {
	Name                 string        `json:"name" mapstructure:"name"`
	Version              uint32        `json:"version" mapstructure:"version"`
	Brokers              []string      `json:"brokers" mapstructure:"brokers"`
	ClusterID            string        `json:"cluster_id" mapstructure:"cluster_id"`
	Topics               []string      `json:"topics" mapstructure:"topics"`
	Plaintext            bool          `json:"plaintext" mapstructure:"plaintext"`
	RootCAPEM            string        `json:"root_ca_pem" mapstructure:"root_ca_pem"`
	SASL                 string        `json:"sasl" mapstructure:"sasl"`
	User                 string        `json:"user" mapstructure:"user"`
	Password             string        `json:"password" mapstructure:"password"`
	TransactionalID      string        `json:"transactional_id" mapstructure:"transactional_id"`
	OffsetGroup          string        `json:"offset_group" mapstructure:"offset_group"`
	ConsumerGroup        string        `json:"consumer_group" mapstructure:"consumer_group"`
	InitialOffset        string        `json:"initial_offset" mapstructure:"initial_offset"`
	ResetOffset          string        `json:"reset_offset" mapstructure:"reset_offset"`
	MaxGroupSessions     int           `json:"max_group_sessions" mapstructure:"max_group_sessions"`
	MaxAssignments       int           `json:"max_assignments" mapstructure:"max_assignments"`
	Routing              string        `json:"routing" mapstructure:"routing"`
	MaxActive            int           `json:"max_active" mapstructure:"max_active"`
	QueuedCalls          int           `json:"queued_calls" mapstructure:"queued_calls"`
	MaxRecords           int           `json:"max_records" mapstructure:"max_records"`
	MaxRecordBytes       int           `json:"max_record_bytes" mapstructure:"max_record_bytes"`
	MaxBatchBytes        int           `json:"max_batch_bytes" mapstructure:"max_batch_bytes"`
	MaxWireBytes         int           `json:"max_wire_bytes" mapstructure:"max_wire_bytes"`
	MaxDecodedBatchBytes int           `json:"max_decoded_batch_bytes" mapstructure:"max_decoded_batch_bytes"`
	MaxDecodedRecords    int           `json:"max_decoded_records" mapstructure:"max_decoded_records"`
	Timeout              time.Duration `json:"timeout_ns" mapstructure:"timeout_ns"`
	CleanupTimeout       time.Duration `json:"cleanup_timeout_ns" mapstructure:"cleanup_timeout_ns"`
	Retries              int           `json:"retries" mapstructure:"retries"`
	Linger               time.Duration `json:"linger_ns" mapstructure:"linger_ns"`
	Compression          string        `json:"compression" mapstructure:"compression"`
}

func options(value Settings) native.OptionsV1 {
	return native.OptionsV1{
		Name:                 value.Name,
		Version:              value.Version,
		Brokers:              value.Brokers,
		ClusterID:            value.ClusterID,
		Topics:               value.Topics,
		Plaintext:            value.Plaintext,
		RootCAPEM:            value.RootCAPEM,
		SASL:                 value.SASL,
		User:                 value.User,
		Password:             value.Password,
		TransactionalID:      value.TransactionalID,
		OffsetGroup:          value.OffsetGroup,
		ConsumerGroup:        value.ConsumerGroup,
		InitialOffset:        value.InitialOffset,
		ResetOffset:          value.ResetOffset,
		MaxGroupSessions:     value.MaxGroupSessions,
		MaxAssignments:       value.MaxAssignments,
		Routing:              value.Routing,
		MaxActive:            value.MaxActive,
		QueuedCalls:          value.QueuedCalls,
		MaxRecords:           value.MaxRecords,
		MaxRecordBytes:       value.MaxRecordBytes,
		MaxBatchBytes:        value.MaxBatchBytes,
		MaxWireBytes:         value.MaxWireBytes,
		MaxDecodedBatchBytes: value.MaxDecodedBatchBytes,
		MaxDecodedRecords:    value.MaxDecodedRecords,
		Timeout:              value.Timeout,
		CleanupTimeout:       value.CleanupTimeout,
		Retries:              value.Retries,
		Linger:               value.Linger,
		Compression:          value.Compression,
	}
}

// Validate prepares frozen settings without constructing clients or performing I/O.
func Validate(value Settings) error {
	_, err := native.Select(options(value))
	return translate(err, "validate")
}

// Dependencies are caller-owned and shared across all sources and overlapping
// generations. Transactions must be the same registry for a given Kafka cluster;
// it enforces only process-local exclusivity, not cross-process remote ownership.
type Dependencies struct {
	Runtime      *adapters.Runtime
	Evidence     *adapters.Inbox[Result]
	Observer     *adapters.Observer
	Transactions *TransactionIDs
}
