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
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
)

func TestDomains(t *testing.T) {
	expected := []failure.DomainRange{
		{Domain: "core", First: 0x000, Last: 0x03F},
		{Domain: "configuration", First: 0x040, Last: 0x07F},
		{Domain: "database", First: 0x080, Last: 0x0FF},
		{Domain: "cache", First: 0x100, Last: 0x13F},
		{Domain: "object_storage", First: 0x140, Last: 0x17F},
		{Domain: "messaging", First: 0x180, Last: 0x1BF},
		{Domain: "orchestration", First: 0x1C0, Last: 0x1FF},
		{Domain: "network", First: 0x200, Last: 0x27F},
		{Domain: "observability", First: 0x280, Last: 0x2BF},
		{Domain: "notification", First: 0x2C0, Last: 0x2FF},
		{Domain: "browser", First: 0x300, Last: 0x31F},
		{Domain: "table_format", First: 0x320, Last: 0x33F},
		{Domain: "application", First: 0x340, Last: 0x37F},
	}
	t.Run("manifest_and_all_facility_representations", func(t *testing.T) {
		if !reflect.DeepEqual(failure.Domains(), expected) {
			t.Fatal("capability allocation changed without migration")
		}
		known := 0
		for number := 0; number <= 0xFFFF; number++ {
			facility := failure.Facility(number)
			var want failure.Domain
			if number > 0 && number <= 0x7FF {
				base := number
				if base >= 0x400 {
					base -= 0x400
				}
				matches := 0
				for _, band := range expected {
					if base >= int(band.First) && base <= int(band.Last) {
						want = band.Domain
						matches++
					}
				}
				if matches > 1 {
					t.Fatal("overlapping domain bands")
				}
			}
			if got := facility.Domain(); got != want {
				t.Fatalf("wrong domain at facility %04X: %q", number, got)
			}
			if want != "" {
				known++
			}
			if facility.Valid() {
				code, err := failure.MakeCode(facility, 1)
				if err != nil || code.Domain() != want {
					t.Fatal("code and facility classification disagree")
				}
			}
		}
		if known != 1791 {
			t.Fatal("wrong assigned domain coverage")
		}
		if failure.Code(0).Domain() != "" || failure.Code(0x80070005).Domain() != "" {
			t.Fatal("native or invalid code classified")
		}
		for _, allocation := range failure.Allocations() {
			if allocation.Facility.Domain() == "" {
				t.Fatal("owner allocation escaped capability policy")
			}
		}
	})
	t.Run("detached_manifest", func(t *testing.T) {
		changed := failure.Domains()
		changed[0].Domain = "mutated"
		changed[1].First = 0
		if !reflect.DeepEqual(failure.Domains(), expected) ||
			failure.ErrCode.Domain() != failure.DomainCore ||
			failure.FacilitySettings.Domain() != failure.DomainConfiguration {
			t.Fatal("mutable domain registry")
		}
	})
	t.Run("reserved_domains_are_not_declarable", func(t *testing.T) {
		for _, facility := range []failure.Facility{0x380, 0x3FF, 0x780, 0x7FF} {
			value := definition()
			value.Code, _ = failure.MakeCode(facility, 1)
			if !value.Code.Valid() || value.Code.Domain() != "" {
				t.Fatal("reserved representation lost")
			}
			if value.Valid() {
				t.Fatal("unassigned domain became a definition")
			}
			if _, err := failure.Prepare(value); !errors.Is(err, failure.ErrDefinition) {
				t.Fatal("reserved domain admitted", err)
			}
			catalog, _ := failure.Prepare()
			if _, found, err := catalog.Lookup(value.Code); err != nil || found {
				t.Fatal("unknown numeric lookup became invalid")
			}
		}
	})
}

func TestCapabilityOwnership(t *testing.T) {
	fixtures := []struct {
		facility  failure.Facility
		module    string
		component string
		domain    failure.Domain
	}{
		{0x481, "example.adapters", "postgresql", failure.DomainDatabase},
		{0x482, "example.framework", "transaction", failure.DomainDatabase},
		{0x501, "example.adapters", "redis", failure.DomainCache},
		{0x541, "example.adapters", "objects", failure.DomainObjectStorage},
		{0x581, "example.adapters", "redis_streams", failure.DomainMessaging},
	}
	var definitions []failure.Definition
	for _, fixture := range fixtures {
		code, err := failure.MakeCode(fixture.facility, 1)
		if err != nil || code.Domain() != fixture.domain {
			t.Fatal("domain depends on layer or brand")
		}
		value := failure.Definition{
			Code: code, Module: fixture.module, Component: fixture.component,
			Identifier: failure.Identifier(fixture.module + "." + fixture.component + ".failed"),
			Revision:   1, Message: "The requested capability failed.",
		}
		if !value.Valid() {
			t.Fatal("capability definition rejected")
		}
		definitions = append(definitions, value)
	}
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	components, err := catalog.Components()
	if err != nil || len(components) != len(fixtures) {
		t.Fatal("missing components")
	}
	for _, component := range components {
		if len(component.Codes) != 1 || component.Domain != component.Codes[0].Domain() ||
			component.Facility != component.Codes[0].Facility() {
			t.Fatal("atlas classification drift")
		}
	}
	database, err := failure.New(definitions[0], failure.Location{Operation: "query"})
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := failure.New(definitions[1], failure.Location{Operation: "transaction"}, database)
	if err != nil || !errors.Is(transaction, definitions[0].Code) ||
		!errors.Is(transaction, definitions[1].Code) ||
		transaction.Diagnostic().Definition.Code.Domain() != failure.DomainDatabase {
		t.Fatal("cross-layer database attribution changed")
	}
}
