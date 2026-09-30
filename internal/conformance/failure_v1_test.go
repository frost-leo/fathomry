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

package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	sdk "github.com/spf13/viper"
)

func TestPublicNumericFailureRetainsNativeEvidence(t *testing.T) {
	definition := failure.Definition{
		Code: 0xA4510001, Identifier: "example.local.invalid_document",
		Module: "example", Component: "local", Revision: 1,
		Message: "The local configuration document is invalid.",
		Details: failure.Contract{ID: "example.local.document_details", Version: 1},
	}
	type details struct {
		Slot string
	}
	input := viper.LoadInput{Options: viper.OptionsV1{Encoding: "json"}, Reader: strings.NewReader(`{"private-key":]}`)}
	_, native := viper.Load(context.Background(), []viper.LoadInput{input})
	var syntax *json.SyntaxError
	var parser sdk.ConfigParseError
	var technical *fault.Error
	if !errors.As(native, &syntax) || !errors.As(native, &parser) || !errors.As(native, &technical) {
		t.Fatal("real native parse-error fixture did not reach expected boundaries")
	}
	if technical.Diagnostic().Context.Operation != "decode" {
		t.Fatal("native phase evidence changed")
	}
	clone := func(value details) details { return value }
	public, err := failure.NewDetailed(definition, failure.Location{Operation: "load", Instance: "primary"}, details{Slot: "base"}, clone, native)
	if err != nil {
		t.Fatal(err)
	}
	var retained *json.SyntaxError
	if !errors.Is(public, definition.Code) || !errors.Is(public, native) ||
		!errors.As(public, &retained) || retained != syntax {
		t.Fatal("public numeric promotion rewrote native identity")
	}
	conformance.Private(t, public, "private-key")
	core, ok := failure.Inspect(public)
	fields, known := public.Details()
	if !ok || !known || fields.Slot != "base" || core.Diagnostic().Location.Instance != "primary" {
		t.Fatal("public semantic context/detail composition lost")
	}
	definition.Code++
	definition.Identifier = "example.local.preparation_failed"
	outer, err := failure.NewDetailed(definition, failure.Location{Operation: "prepare"}, details{Slot: "complete"}, clone, public)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := failure.Inspect(outer)
	outerDetails, _ := outer.Details()
	if !ok || current.Diagnostic().Definition.Code != definition.Code || outerDetails.Slot != "complete" ||
		!errors.Is(outer, public) || !errors.As(outer, &retained) || retained != syntax {
		t.Fatal("second semantic layer obscured origin or borrowed descendant facts")
	}
	valid := viper.LoadInput{Options: viper.OptionsV1{Encoding: "json"}, Reader: strings.NewReader(`{"value":1}`)}
	if _, err := viper.Load(context.Background(), []viper.LoadInput{valid}); err != nil {
		t.Fatal("native valid-input control failed", err)
	}
}
