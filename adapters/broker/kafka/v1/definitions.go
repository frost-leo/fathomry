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

import "github.com/frost-leo/fathomry/failure/v1"

// Stable codes identify technical meaning, never retry or effect policy.
const (
	ErrInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0001
	ErrUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0002
	ErrAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0003
	ErrConnect       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0004
	ErrIdentity      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0005
	ErrProduce       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0006
	ErrRead          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0007
	ErrMissing       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0008
	ErrUnavailable   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0009
	ErrExpired       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000a
	ErrLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000b
	ErrState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000c
	ErrTransaction   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000d
	ErrOffsets       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000e
	ErrCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x000f
	ErrSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityKafka)<<16 | 0x0010
)

// Definitions returns detached offline declarations; it never opens a client.
func Definitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrUnsupported), definition(ErrAuthority), definition(ErrConnect), definition(ErrIdentity), definition(ErrProduce), definition(ErrRead), definition(ErrMissing), definition(ErrUnavailable), definition(ErrExpired), definition(ErrLimit), definition(ErrState), definition(ErrTransaction), definition(ErrOffsets), definition(ErrCleanup), definition(ErrSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput:
		identifier, message = "invalid_input", "The Kafka input or settings are invalid."
	case ErrUnsupported:
		identifier, message = "unsupported_profile", "The requested Kafka profile is unsupported."
	case ErrAuthority:
		identifier, message = "authority_refused", "The Kafka endpoint is outside the permitted authority."
	case ErrConnect:
		identifier, message = "connection_failed", "The Kafka connection or readiness check failed."
	case ErrIdentity:
		identifier, message = "identity_mismatch", "The Kafka cluster, topic or assignment identity does not match."
	case ErrProduce:
		identifier, message = "production_failed", "Kafka production failed; inspect per-record effect evidence."
	case ErrRead:
		identifier, message = "read_failed", "The Kafka read failed; partial observations may remain."
	case ErrMissing:
		identifier, message = "record_missing", "The exact Kafka offset has no visible record."
	case ErrUnavailable:
		identifier, message = "record_unavailable", "Kafka data or assignment is not currently available."
	case ErrExpired:
		identifier, message = "offset_expired", "The requested Kafka offset is outside the observed log range."
	case ErrLimit:
		identifier, message = "limit_exceeded", "A Kafka capability bound was exceeded."
	case ErrState:
		identifier, message = "invalid_state", "The Kafka owner or assignment cannot perform this operation."
	case ErrTransaction:
		identifier, message = "transaction_failed", "The Kafka transaction failed; its final outcome is separate."
	case ErrOffsets:
		identifier, message = "checkpoint_failed", "The Kafka checkpoint operation failed; inspect commit evidence."
	case ErrCleanup:
		identifier, message = "cleanup_failed", "Kafka cleanup reported a failure; completion is separate."
	case ErrSerialization:
		identifier, message = "runtime_serialization", "Kafka runtime value serialization is unsupported."
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry.broker_kafka." + identifier),
		Module: "fathomry", Component: "broker_kafka", Revision: 1, Message: message}
}
