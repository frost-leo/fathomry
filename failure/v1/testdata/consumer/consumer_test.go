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

package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
)

const unavailable failure.Code = 0xA4490001

func TestCapabilityCodeDomains(t *testing.T) {
	var definitions []failure.Definition
	for _, item := range []struct {
		facility  failure.Facility
		component string
		domain    failure.Domain
	}{
		{0x481, "database", failure.DomainDatabase},
		{0x501, "cache", failure.DomainCache},
		{0x541, "objects", failure.DomainObjectStorage},
	} {
		code, err := failure.MakeCode(item.facility, 1)
		if err != nil || code.Domain() != item.domain {
			t.Fatal("public code did not identify the capability")
		}
		definitions = append(definitions, failure.Definition{Code: code, Module: "customer", Component: item.component,
			Identifier: failure.Identifier("customer." + item.component + ".failed"), Revision: 1, Message: "The selected capability failed."})
	}
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	components, err := catalog.Components()
	if err != nil || len(components) != 3 {
		t.Fatal("missing capability atlas components")
	}
	for _, component := range components {
		if component.Domain == "" || component.Domain != component.Codes[0].Domain() {
			t.Fatal("component atlas lost its capability domain")
		}
	}
}

type nativeStatus uint64

func (nativeStatus) Error() string { return "native status" }

func TestNativeNumericDomainIsNotNarrowed(t *testing.T) {
	const native nativeStatus = 0x100000005
	value, err := failure.New(declaration(), failure.Location{}, native)
	if err != nil {
		t.Fatal(err)
	}
	var retained nativeStatus
	if !errors.As(value, &retained) || retained != native || !errors.Is(value, native) || !errors.Is(value, unavailable) {
		t.Fatal("32-bit public code changed native numeric evidence")
	}
}

type ConfigurationDetails struct {
	Document string   `json:"document"`
	Fields   []string `json:"fields"`
}

func declaration() failure.Definition {
	return failure.Definition{
		Code: unavailable, Identifier: "customer.configsource.unavailable",
		Module: "customer", Component: "configsource", Revision: 1,
		Message: "The selected configuration source is unavailable.",
		Details: failure.Contract{ID: "customer.configsource.acquisition", Version: 1},
	}
}

func TestIndependentErrorAndCatalog(t *testing.T) {
	input := ConfigurationDetails{Document: "private-document", Fields: []string{"private-field"}}
	native := &fs.PathError{Op: "read", Path: "private-path", Err: fs.ErrPermission}
	value, err := failure.NewDetailed(declaration(), failure.Location{Operation: "read", Instance: "primary"}, input, func(value ConfigurationDetails) ConfigurationDetails {
		value.Fields = slices.Clone(value.Fields)
		return value
	}, native)
	if err != nil {
		t.Fatal(err)
	}
	input.Fields[0] = "changed"
	own, ok := failure.Inspect(value)
	if !ok || own != value.Failure() || own.Diagnostic().Definition.Code != unavailable {
		t.Fatal("public composition failed")
	}
	fields, ok := value.Details()
	if !ok || !reflect.DeepEqual(fields, ConfigurationDetails{Document: "private-document", Fields: []string{"private-field"}}) {
		t.Fatal("public detail type/storage lost")
	}
	var original *fs.PathError
	if !errors.As(value, &original) || original != native || !errors.Is(value, unavailable) || !errors.Is(value, fs.ErrPermission) {
		t.Fatal("original native cause or numeric identity lost")
	}
	for _, canary := range []string{"private-document", "private-field", "private-path"} {
		if strings.Contains(fmt.Sprintf("%+v", value), canary) {
			t.Fatal("default diagnostic disclosed runtime data")
		}
	}
	selected := append(failure.Definitions(), declaration())
	catalog, err := failure.Prepare(selected...)
	if err != nil {
		t.Fatal(err)
	}
	numeric, exists, err := catalog.Lookup(unavailable)
	if err != nil || !exists {
		t.Fatal("numeric atlas query failed", err)
	}
	symbolic, exists, err := catalog.LookupIdentifier("customer.configsource.unavailable")
	if err != nil || !exists || numeric != symbolic {
		t.Fatal("identifier lookup disagreed", err)
	}
	owners, err := catalog.Components()
	if err != nil || len(owners) != 2 {
		t.Fatal("definition owners unavailable", err)
	}
	other := declaration()
	other.Identifier = "customer.configsource.other"
	if _, err := failure.Prepare(declaration(), other); !errors.Is(err, failure.ErrDefinition) {
		t.Fatal("collision was silently overridden", err)
	}
	bytes, err := json.Marshal(unavailable)
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip failure.Code
	if err := json.Unmarshal(bytes, &roundtrip); err != nil || roundtrip != unavailable {
		t.Fatal("high-bit numeric identity was rounded", err)
	}
	if _, err := json.Marshal(value); !errors.Is(err, failure.ErrSerialization) {
		t.Fatal("runtime error acquired a wire schema", err)
	}
	for _, wrapped := range []error{fmt.Errorf("context: %w", value), errors.Join(value, native)} {
		if _, ok := failure.Inspect(wrapped); ok {
			t.Fatal("selected a descendant occurrence implicitly")
		}
	}
}

type customError struct {
	core *failure.Error
	at   time.Time
	note string
}

func (value *customError) Failure() *failure.Error {
	if value == nil {
		return nil
	}
	return value.core
}
func (value *customError) Error() string { return value.Failure().Error() }
func (value *customError) Unwrap() error {
	if core := value.Failure(); core != nil {
		return core
	}
	return nil
}
func (value customError) Format(state fmt.State, verb rune) {
	if value.core == nil {
		_, _ = fmt.Fprint(state, "<nil>")
		return
	}
	value.core.Format(state, verb)
}
func (value *customError) LogValue() slog.Value  { return value.Failure().LogValue() }
func (customError) MarshalJSON() ([]byte, error) { return nil, failure.ErrSerialization }
func (*customError) UnmarshalJSON([]byte) error  { return failure.ErrSerialization }

func TestOwnerDefinedOccurrenceWithoutDetailed(t *testing.T) {
	native := &fs.PathError{Op: "open", Path: "private-path", Err: fs.ErrPermission}
	core, err := failure.New(declaration(), failure.Location{Operation: "load"}, native)
	if err != nil {
		t.Fatal("component could not construct its own declared-detail core", err)
	}
	instant := time.Date(2026, 9, 29, 0, 0, 0, 0, time.UTC)
	value := &customError{core: core, at: instant, note: "private-component-note"}
	selected, ok := failure.Inspect(value)
	if !ok || selected != core || selected.Diagnostic().Definition.Details != declaration().Details || value.at != instant {
		t.Fatal("custom occurrence failed the shared protocol")
	}
	if !errors.Is(value, unavailable) || !errors.Is(value, native) {
		t.Fatal("custom occurrence lost semantic or native identity")
	}
	var original *fs.PathError
	if !errors.As(value, &original) || original != native {
		t.Fatal("custom occurrence rewrote native cause")
	}
	if strings.Contains(fmt.Sprintf("%+v", value), "private-component-note") {
		t.Fatal("component presentation exposed its private extension")
	}
	var absent *customError
	if selected, ok := failure.Inspect(absent); selected != nil || ok {
		t.Fatal("nil custom occurrence was inspected")
	}
}
