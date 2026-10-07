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

package failure

import "strings"

// Facility is an 11-bit stable logical subsystem owner within a capability Domain,
// not an Adapter/Framework/CLI layer, SDK version, instance, import-path hash or
// runtime index. Zero and values above 0x7FF are invalid. An assigned number must
// never be reused for another owner.
//
// Allocation rules for every new component:
//
//  1. Choose the semantic capability in Domains first (database, cache, object
//     storage, etc.), then assign a subsystem slot inside that capability's band.
//     Never bring back separate Adapter, Framework or CLI allocation bands.
//  2. First-party slots below 0x400 require an explicit constant and an Allocations
//     entry binding the exact logical Module/Component. An unlisted slot is reserved,
//     not permission to self-assign. Package moves and SDK upgrades do not renumber it.
//  3. Extensions use the corresponding capability band plus 0x400. Coordinate slots
//     with the target catalog owner; the offset denotes a separate allocation area,
//     not a shared subsystem or globally collision-free third-party registration.
//  4. Allocate local Number values explicitly from 1 through 65535. Do not use iota,
//     hashes, runtime counters or initialization order. Published numbers/owners
//     are never recycled; keep retired assignments reserved as tombstones.
//  5. Keep one facility per logical owner and one owner per facility in a catalog.
//     New error meanings require new local numbers, not a locale, log level or retry
//     flag change. Preserve native causes and extensible details independently.
//
// Review additions against Code, Domains and the allocation/domain tests together.
// docs/reference/failure/v1/code-allocation.md explains the same protocol; the
// executable manifests below and in domains.go remain the authoritative number tables.
type Facility uint16

const (
	// FirstExtensionFacility begins the project-coordinated extension range.
	// Its capability bands mirror the first-party bands at this offset.
	FirstExtensionFacility Facility = 0x400
	// MaxFacility is the largest representable facility.
	MaxFacility Facility = 0x7FF

	// FacilityFailure belongs to fathomry/failure.
	FacilityFailure Facility = 0x001
	// FacilityI18n belongs to fathomry/i18n in the core capability domain.
	FacilityI18n Facility = 0x003
	// FacilityResource belongs to fathomry/resource in the core capability domain.
	FacilityResource Facility = 0x004
	// FacilityOperation belongs to the shared public operation capability.
	// Its adapters/v1 location does not make this a code-layer allocation.
	FacilityOperation Facility = 0x005
	// FacilitySettings belongs to fathomry/settings in the configuration domain.
	// Allocation is not a claim that every capability of the module is implemented.
	FacilitySettings Facility = 0x041
	// FacilityConfigurationData owns strict configuration preparation.
	FacilityConfigurationData Facility = 0x042
	// FacilityViper owns the public local/native Viper configuration capability.
	FacilityViper Facility = 0x043
	// FacilityNacos owns the public Nacos configuration capability.
	FacilityNacos Facility = 0x044
	// FacilityAssembly owns public runtime composition, not a Framework layer band.
	FacilityAssembly Facility = 0x006
	// FacilityCommandLine owns command invocation and terminal I/O, not a CLI layer band.
	FacilityCommandLine Facility = 0x007
	// FacilityConfiguration owns typed configuration acceptance and publication.
	FacilityConfiguration Facility = 0x045
	// FacilityProjectCreation owns independent project creation and its effects.
	FacilityProjectCreation Facility = 0x00A
	// FacilityPostgreSQL owns the public PostgreSQL database capability.
	FacilityPostgreSQL Facility = 0x080
	// FacilityMySQL owns the public MySQL database capability.
	FacilityMySQL Facility = 0x081
	// FacilityDuckDB owns the public local DuckDB database capability.
	FacilityDuckDB Facility = 0x082
	// FacilityTrino owns the public Trino database capability.
	FacilityTrino Facility = 0x083
	// FacilityMinIO owns the public object-storage MinIO capability.
	FacilityMinIO Facility = 0x140
	// FacilityKafka owns the public Kafka broker capability.
	FacilityKafka Facility = 0x180
	// FacilityRedisCache owns Redis key-value/cache semantics.
	FacilityRedisCache Facility = 0x100
	// FacilityRedisMessaging owns Redis Streams and messaging semantics.
	FacilityRedisMessaging Facility = 0x181
)

// Valid checks the numeric field, not assignment or namespace authority.
func (facility Facility) Valid() bool { return facility > 0 && facility <= MaxFacility }

// Extension reports a valid application/extension-range facility. Authors must
// coordinate these numbers with the catalog owner before publishing constants;
// this range is not a globally collision-free autonomous registration scheme.
func (facility Facility) Extension() bool {
	return facility >= FirstExtensionFacility && facility <= MaxFacility
}

// Allocation binds a first-party facility to its permanent logical owner.
// Retired allocations remain reserved; absence means unassigned, not available
// for arbitrary component use. This is identity metadata, not a runtime registry.
type Allocation struct {
	Facility  Facility `json:"facility"`
	Module    string   `json:"module"`
	Component string   `json:"component"`
}

// Allocations returns the detached first-party subsystem manifest. Future modules
// add reviewed entries within their capability bands from Domains, regardless of
// which public layer implements them. Extension owners are discovered from the
// explicitly prepared Catalog instead. Unassigned subsystem slots remain reserved.
func Allocations() []Allocation {
	return []Allocation{
		{Facility: FacilityFailure, Module: "fathomry", Component: "failure"},
		{Facility: FacilityI18n, Module: "fathomry", Component: "i18n"},
		{Facility: FacilityResource, Module: "fathomry", Component: "resource"},
		{Facility: FacilityOperation, Module: "fathomry", Component: "operation"},
		{Facility: FacilitySettings, Module: "fathomry", Component: "settings"},
		{Facility: FacilityConfigurationData, Module: "fathomry", Component: "configuration_data"},
		{Facility: FacilityViper, Module: "fathomry", Component: "configsource_viper"},
		{Facility: FacilityNacos, Module: "fathomry", Component: "configsource_nacos"},
		{Facility: FacilityAssembly, Module: "fathomry", Component: "assembly"},
		{Facility: FacilityConfiguration, Module: "fathomry", Component: "configuration"},
		{Facility: FacilityCommandLine, Module: "fathomry", Component: "command_line"},
		{Facility: FacilityProjectCreation, Module: "fathomry", Component: "project_creation"},
		{Facility: FacilityPostgreSQL, Module: "fathomry", Component: "database_postgres"},
		{Facility: FacilityMySQL, Module: "fathomry", Component: "database_mysql"},
		{Facility: FacilityTrino, Module: "fathomry", Component: "database_trino"},
		{Facility: FacilityMinIO, Module: "fathomry", Component: "objectstore_minio"},
		{Facility: FacilityKafka, Module: "fathomry", Component: "broker_kafka"},
		{Facility: FacilityRedisCache, Module: "fathomry", Component: "cache_redis"},
		{Facility: FacilityRedisMessaging, Module: "fathomry", Component: "messaging_redis"},
		{Facility: FacilityDuckDB, Module: "fathomry", Component: "database_duckdb"},
	}
}

func ownsFacility(definition Definition) bool {
	facility := definition.Code.Facility()
	if facility.Domain() == "" {
		return false
	}
	if facility.Extension() {
		return definition.Module != "fathomry" && !strings.HasPrefix(definition.Module, "fathomry.")
	}
	for _, allocation := range Allocations() {
		if facility == allocation.Facility {
			return definition.Module == allocation.Module && definition.Component == allocation.Component
		}
	}
	return false
}
