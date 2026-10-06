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

package redis

import "github.com/frost-leo/fathomry/failure/v1"

// Stable codes follow semantic capability, not SDK brand. Cache owns source
// preparation/credential/lifecycle failures; messaging roots/children have their
// own codes. Forwarded public operation/resource/settings errors are unchanged.
const (
	ErrInput                  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0001
	ErrMessagingInput         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0001
	ErrAuthority              failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0002
	ErrMessagingAuthority     failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0002
	ErrUnsupported            failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0003
	ErrMessagingUnsupported   failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0003
	ErrLimit                  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0004
	ErrMessagingLimit         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0004
	ErrProtocol               failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0005
	ErrMessagingProtocol      failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0005
	ErrCommand                failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0006
	ErrMessagingCommand       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0006
	ErrState                  failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0007
	ErrMessagingState         failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0007
	ErrCleanup                failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0008
	ErrMessagingCleanup       failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0008
	ErrSerialization          failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisCache)<<16 | 0x0009
	ErrMessagingSerialization failure.Code = failure.ErrorPrefix | failure.Code(failure.FacilityRedisMessaging)<<16 | 0x0009
)

// Definitions returns detached declarations for both capability owners.
func Definitions() []failure.Definition {
	return append(CacheDefinitions(), MessagingDefinitions()...)
}
func CacheDefinitions() []failure.Definition {
	return []failure.Definition{definition(ErrInput), definition(ErrAuthority), definition(ErrUnsupported), definition(ErrLimit), definition(ErrProtocol), definition(ErrCommand), definition(ErrState), definition(ErrCleanup), definition(ErrSerialization)}
}
func MessagingDefinitions() []failure.Definition {
	return []failure.Definition{definition(ErrMessagingInput), definition(ErrMessagingAuthority), definition(ErrMessagingUnsupported), definition(ErrMessagingLimit), definition(ErrMessagingProtocol), definition(ErrMessagingCommand), definition(ErrMessagingState), definition(ErrMessagingCleanup), definition(ErrMessagingSerialization)}
}
func definition(code failure.Code) failure.Definition {
	var identifier, message string
	switch code {
	case ErrInput, ErrMessagingInput:
		identifier, message = "invalid_input", "The Redis input or settings are invalid."
	case ErrAuthority, ErrMessagingAuthority:
		identifier, message = "authority_refused", "The Redis capability or endpoint authority was refused."
	case ErrUnsupported, ErrMessagingUnsupported:
		identifier, message = "unsupported_profile", "The requested Redis profile is unsupported."
	case ErrLimit, ErrMessagingLimit:
		identifier, message = "limit_exceeded", "A Redis capability bound was exceeded."
	case ErrProtocol, ErrMessagingProtocol:
		identifier, message = "protocol_failed", "A Redis reply was malformed or could not be retained."
	case ErrCommand, ErrMessagingCommand:
		identifier, message = "command_failed", "A Redis command failed; inspect individual reply and effect facts."
	case ErrState, ErrMessagingState:
		identifier, message = "invalid_state", "The Redis owner cannot perform this operation."
	case ErrCleanup, ErrMessagingCleanup:
		identifier, message = "cleanup_failed", "Redis cleanup failed; actual release is reported separately."
	case ErrSerialization, ErrMessagingSerialization:
		identifier, message = "runtime_serialization", "Redis runtime value serialization is unsupported."
	}
	component := "cache_redis"
	if code.Facility() == failure.FacilityRedisMessaging {
		component = "messaging_redis"
	}
	return failure.Definition{Code: code, Identifier: failure.Identifier("fathomry." + component + "." + identifier),
		Module: "fathomry", Component: component, Revision: 1, Message: message}
}
