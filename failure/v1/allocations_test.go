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

package failure_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func TestCodeLayout(t *testing.T) {
	for facility := failure.Facility(1); facility <= failure.MaxFacility; facility++ {
		for _, number := range []uint16{1, 0x7FFF, 0x8000, 0xFFFF} {
			code, err := failure.MakeCode(facility, number)
			expected := uint32(0xA0000000) | uint32(facility)<<16 | uint32(number)
			if err != nil || !code.Valid() || uint32(code) != expected || code.Facility() != facility || code.Number() != number {
				t.Fatal("composition or extraction changed fields")
			}
			for bit := 27; bit < 32; bit++ {
				invalid := code ^ (1 << bit)
				if invalid.Valid() || invalid.Facility() != 0 || invalid.Number() != 0 {
					t.Fatal("invalid flag admitted")
				}
			}
		}
	}
	for _, facility := range []failure.Facility{0, 0x800, 0xFFFF} {
		if code, err := failure.MakeCode(facility, 1); code != 0 || !errors.Is(err, failure.ErrCode) {
			t.Fatal("facility silently masked")
		}
	}
	if code, err := failure.MakeCode(1, 0); code != 0 || !errors.Is(err, failure.ErrCode) {
		t.Fatal("zero local number admitted")
	}
	for _, code := range []failure.Code{0, 1, 0x80070005, 0xC0000005, 0xA0000001, 0xA0010000, 0xFFFFFFFF} {
		if code.Valid() || code.Facility() != 0 || code.Number() != 0 {
			t.Fatal("native/zero/reserved identity admitted")
		}
	}
	if failure.Code(0).String() != "0x00000000" {
		t.Fatal("zero diagnostic width changed")
	}
}

func TestAllocations(t *testing.T) {
	t.Run("stable_first_party_manifest", func(t *testing.T) {
		allocations := failure.Allocations()
		if len(allocations) < 14 || allocations[0] != (failure.Allocation{Facility: 1, Module: "fathomry", Component: "failure"}) ||
			allocations[1] != (failure.Allocation{Facility: 0x003, Module: "fathomry", Component: "i18n"}) ||
			allocations[2] != (failure.Allocation{Facility: 0x004, Module: "fathomry", Component: "resource"}) ||
			allocations[3] != (failure.Allocation{Facility: 0x005, Module: "fathomry", Component: "operation"}) ||
			allocations[4] != (failure.Allocation{Facility: 0x041, Module: "fathomry", Component: "settings"}) ||
			allocations[5] != (failure.Allocation{Facility: 0x042, Module: "fathomry", Component: "configuration_data"}) ||
			allocations[6] != (failure.Allocation{Facility: 0x043, Module: "fathomry", Component: "configsource_viper"}) ||
			allocations[7] != (failure.Allocation{Facility: 0x044, Module: "fathomry", Component: "configsource_nacos"}) ||
			allocations[8] != (failure.Allocation{Facility: 0x006, Module: "fathomry", Component: "assembly"}) ||
			allocations[9] != (failure.Allocation{Facility: 0x045, Module: "fathomry", Component: "configuration"}) ||
			allocations[10] != (failure.Allocation{Facility: 0x007, Module: "fathomry", Component: "command_line"}) ||
			allocations[11] != (failure.Allocation{Facility: 0x00A, Module: "fathomry", Component: "project_creation"}) ||
			allocations[12] != (failure.Allocation{Facility: 0x080, Module: "fathomry", Component: "database_postgres"}) ||
			allocations[13] != (failure.Allocation{Facility: 0x081, Module: "fathomry", Component: "database_mysql"}) {
			t.Fatal("allocation manifest changed without migration")
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilityKafka, Module: "fathomry", Component: "broker_kafka"}) || failure.FacilityKafka != 0x180 || failure.FacilityKafka.Domain() != failure.DomainMessaging {
			t.Fatal("Kafka capability allocation changed")
		}
		for _, expected := range []failure.Allocation{
			{Facility: failure.FacilityRedisCache, Module: "fathomry", Component: "cache_redis"},
			{Facility: failure.FacilityRedisMessaging, Module: "fathomry", Component: "messaging_redis"},
		} {
			if !slices.Contains(allocations, expected) {
				t.Fatal("Redis capability allocation missing")
			}
		}
		if failure.FacilityRedisCache != 0x100 || failure.FacilityRedisMessaging != 0x181 ||
			failure.FacilityRedisCache.Domain() != failure.DomainCache || failure.FacilityRedisMessaging.Domain() != failure.DomainMessaging {
			t.Fatal("Redis capability domain changed")
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilityDuckDB, Module: "fathomry", Component: "database_duckdb"}) || failure.FacilityDuckDB != 0x082 || failure.FacilityDuckDB.Domain() != failure.DomainDatabase {
			t.Fatal("DuckDB capability allocation changed")
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilityTrino, Module: "fathomry", Component: "database_trino"}) || failure.FacilityTrino != 0x083 || failure.FacilityTrino.Domain() != failure.DomainDatabase {
			t.Fatal("Trino capability allocation changed")
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilityDoris, Module: "fathomry", Component: "database_doris"}) || failure.FacilityDoris != 0x084 || failure.FacilityDoris.Domain() != failure.DomainDatabase {
			t.Fatal("Doris capability allocation changed")
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilityNetHTTP, Module: "fathomry", Component: "http_nethttp"}) ||
			failure.FacilityNetHTTP != 0x200 || failure.FacilityNetHTTP.Domain() != failure.DomainNetwork {
			t.Fatal("nethttp capability allocation changed")
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilitySurf, Module: "fathomry", Component: "http_surf"}) ||
			failure.FacilitySurf != 0x202 || failure.FacilitySurf.Domain() != failure.DomainNetwork {
			t.Fatal("Surf capability allocation changed")
		}
		for index, allocation := range allocations {
			if allocation.Facility == failure.FacilityTLSClient && (allocation.Module != "fathomry" || allocation.Component != "http_tlsclient" || allocation.Facility != 0x201 || allocation.Facility.Domain() != failure.DomainNetwork) {
				t.Fatal("tlsclient capability allocation changed")
			}
			for _, earlier := range allocations[:index] {
				if allocation.Facility == earlier.Facility || allocation.Module == earlier.Module && allocation.Component == earlier.Component {
					t.Fatal("first-party facility ownership is not unique")
				}
			}
		}
		if !slices.Contains(allocations, failure.Allocation{Facility: failure.FacilityTLSClient, Module: "fathomry", Component: "http_tlsclient"}) {
			t.Fatal("tlsclient allocation missing")
		}
		if failure.ErrCode != 0xA0010001 || failure.ErrSerialization != 0xA0010007 {
			t.Fatal("published local numbers changed")
		}
		allocations[0].Module = "mutated"
		if failure.Allocations()[0].Module != "fathomry" || !failure.Definitions()[0].Valid() {
			t.Fatal("mutable allocation alias")
		}
		for _, facility := range []failure.Facility{0, 0x800, 0xFFFF} {
			if facility.Valid() || facility.Extension() {
				t.Fatal("invalid facility range")
			}
		}
		for _, facility := range []failure.Facility{1, 0xFF, 0x100, 0x2FF, 0x300, 0x3FF} {
			if !facility.Valid() || facility.Extension() {
				t.Fatal("first-party facility classification")
			}
		}
		for _, facility := range []failure.Facility{0x400, 0x7FF} {
			if !facility.Valid() || !facility.Extension() {
				t.Fatal("extension facility classification")
			}
		}
	})
	t.Run("first_party_owner_and_reservations", func(t *testing.T) {
		for name, modify := range map[string]func(*failure.Definition){
			"old_settings_slot": func(d *failure.Definition) {
				d.Code = 0xA0020001
				d.Module = "fathomry"
				d.Component = "settings"
				d.Identifier = "fathomry.settings.invalid_snapshot"
			},
			"reserved_domain":               func(d *failure.Definition) { d.Code = 0xA7800001 },
			"steal_assigned":                func(d *failure.Definition) { d.Code = 0xA0010010 },
			"use_unassigned":                func(d *failure.Definition) { d.Code = 0xA1010001 },
			"first_party_name_in_extension": func(d *failure.Definition) { d.Module = "fathomry"; d.Identifier = "fathomry.source.read_failed" },
			"first_party_submodule_in_extension": func(d *failure.Definition) {
				d.Module = "fathomry.custom"
				d.Identifier = "fathomry.custom.source.read_failed"
			},
		} {
			t.Run(name, func(t *testing.T) {
				value := definition()
				modify(&value)
				if value.Valid() {
					t.Fatal("invalid owner admitted")
				}
				if occurrence, err := failure.New(value, failure.Location{}); occurrence != nil || !errors.Is(err, failure.ErrDefinition) {
					t.Fatal("constructor skipped allocation", err)
				}
				if catalog, err := failure.Prepare(value); catalog != nil || !errors.Is(err, failure.ErrDefinition) {
					t.Fatal("catalog skipped allocation", err)
				}
			})
		}
		value := definition()
		value.Code = 0xA0410001
		value.Module = "fathomry"
		value.Component = "settings"
		value.Identifier = "fathomry.settings.invalid_snapshot"
		if !value.Valid() {
			t.Fatal("declared owner cannot use its allocation")
		}
		unknown, err := failure.MakeCode(0x123, 1)
		if err != nil || !unknown.Valid() {
			t.Fatal("well-formed unknown code cannot be queried")
		}
		empty, _ := failure.Prepare()
		if _, found, err := empty.Lookup(unknown); err != nil || found {
			t.Fatal("unknown well-formed code is not absence")
		}
	})
	t.Run("bijective_facility_ownership_per_catalog", func(t *testing.T) {
		for _, sameFacility := range []bool{true, false} {
			first, second := definition(), definition()
			second.Code++
			second.Identifier = "example.source.second"
			if sameFacility {
				second.Component = "other"
				second.Identifier = "example.other.second"
			} else {
				second.Code = 0xA4010002
			}
			if !first.Valid() || !second.Valid() {
				t.Fatal("counterexample not individually valid")
			}
			if catalog, err := failure.Prepare(first, second); catalog != nil || !errors.Is(err, failure.ErrDefinition) {
				t.Fatal("ambiguous facility ownership admitted", err)
			}
		}
		first, second := definition(), definition()
		second.Code++
		second.Identifier = "example.source.second"
		if _, err := failure.Prepare(first, second); err != nil {
			t.Fatal("one owner cannot declare multiple local errors", err)
		}
	})
}
