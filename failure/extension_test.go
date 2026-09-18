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
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/failure/testdata/configcheck"
	"github.com/frost-leo/fathomry/failure/testdata/orders"
)

//go:embed testdata/presentation.json
var presentationResources []byte

type commonOccurrence = failure.Error
type refinedFailure struct {
	commonOccurrence
	rule string
}

func TestIndependentSemanticsAndRequiredStructuredDetail(t *testing.T) {
	input := []configcheck.Violation{{Field: "port", Rule: "type"}, {Field: "enabled", Rule: "type"}}
	validation := configcheck.NewValidation(input)
	input[0].Field = "changed"
	limit := orders.NewLimit(0, true)
	for _, test := range []struct {
		err  error
		code failure.Code
	}{
		{validation, configcheck.InvalidInput}, {&validation, configcheck.InvalidInput},
		{limit, orders.Throttled}, {&limit, orders.Throttled},
	} {
		occurrence, ok := failure.Inspect(test.err)
		if !ok || occurrence.Code() != test.code || !errors.Is(test.err, test.code) {
			t.Fatal("independent semantic identity lost")
		}
		var common failure.Error
		if !errors.As(test.err, &common) || common.Code() != test.code {
			t.Fatal("typed extension lost common inspection contract")
		}
		if got := fmt.Sprintf("%#v", test.err); got != string(test.code) {
			t.Fatal("extension lost safe sharp formatting")
		}
		if _, err := json.Marshal(test.err); err == nil {
			t.Fatal("extension acquired accidental runtime JSON")
		}
		var log bytes.Buffer
		slog.New(slog.NewJSONHandler(&log, nil)).Info("fixture", slog.Any("error", test.err))
		if !strings.Contains(log.String(), string(test.code)) ||
			strings.Contains(log.String(), "violations") || strings.Contains(log.String(), "PANIC") {
			t.Fatal("extension lost the common structured logging projection")
		}
	}
	want := []configcheck.Violation{{Field: "port", Rule: "type"}, {Field: "enabled", Rule: "type"}}
	got, complete := validation.Violations()
	if !complete || !reflect.DeepEqual(got, want) {
		t.Fatal("required collection changed")
	}
	got[0].Field = "changed-again"
	again, _ := validation.Violations()
	if !reflect.DeepEqual(again, want) {
		t.Fatal("required detail aliases caller storage")
	}
	for _, input := range []string{"{}", "null"} {
		copy := validation
		if json.Unmarshal([]byte(input), &copy) == nil {
			t.Fatal("typed extension reconstructed from runtime JSON")
		}
		detail, complete := copy.Violations()
		if !complete || !reflect.DeepEqual(detail, want) {
			t.Fatal("failed extension reconstruction changed required detail")
		}
	}

	for _, invalid := range [][]configcheck.Violation{
		nil, {}, {{Field: "secret-private-value", Rule: "type"}},
		{{Field: "port", Rule: "unsupported"}},
		make([]configcheck.Violation, configcheck.MaxViolations+1),
	} {
		err := configcheck.NewValidation(invalid)
		list, complete := err.Violations()
		if !errors.Is(err, configcheck.InvalidInput) || complete || len(list) != 0 {
			t.Fatal("required-detail refusal replaced primary or claimed completeness")
		}
		if strings.Contains(fmt.Sprintf("%#v", err), "secret-private-value") {
			t.Fatal("invalid required detail leaked")
		}
	}
	exact := make([]configcheck.Violation, configcheck.MaxViolations)
	for index := range exact {
		exact[index] = want[0]
	}
	if _, complete := configcheck.NewValidation(exact).Violations(); !complete {
		t.Fatal("exact required-detail collection bound refused")
	}

	for _, test := range []struct {
		delay          time.Duration
		present, valid bool
	}{
		{0, false, true}, {0, true, true}, {time.Hour, true, true},
		{-1, true, false}, {time.Hour + 1, true, false}, {1, false, false},
	} {
		err := orders.NewLimit(test.delay, test.present)
		delay, present, valid := err.RetryAfter()
		if !errors.Is(err, orders.Throttled) || valid != test.valid ||
			valid && (delay != test.delay || present != test.present) {
			t.Fatal("hint validity, absent or present zero semantics changed")
		}
	}
	var typed configcheck.Validation
	if !errors.As(fmt.Errorf("caller: %w", validation), &typed) {
		t.Fatal("public typed detail lost through ordinary wrapping")
	}
	var nilExtension *configcheck.Validation
	if _, ok := failure.Inspect(nilExtension); ok {
		t.Fatal("nil extension described as occurrence")
	}

	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 100 {
				list, complete := validation.Violations()
				if !complete || !reflect.DeepEqual(list, want) {
					t.Error("concurrent detail changed")
				}
				list[0].Rule = "caller-change"
			}
		})
	}
	group.Wait()
}

func TestCompatibleRefinementPreservesOldClientContract(t *testing.T) {
	const oldCode failure.Code = "example.query.unsupported"
	const differentMeaning failure.Code = "example.query.invalid"
	reason := errors.New("declared public reason")
	legacyClient := func(err error) bool {
		var common failure.Error
		return errors.Is(err, oldCode) && errors.Is(err, reason) &&
			errors.As(err, &common) && common.Code() == oldCode
	}
	before := failure.New(oldCode, reason)
	after := refinedFailure{
		commonOccurrence: failure.New(oldCode, reason, failure.Attribute{Name: "operator", Value: "contains"}),
		rule:             "operator_not_supported",
	}
	if !legacyClient(before) || !legacyClient(after) {
		t.Fatal("additive refinement broke the old client")
	}
	if after.Unwrap() != reason {
		t.Fatal("refinement introduced a fake classification cause")
	}
	var newer refinedFailure
	if !errors.As(after, &newer) || newer.rule != "operator_not_supported" {
		t.Fatal("refinement unavailable")
	}
	if legacyClient(failure.New(differentMeaning, reason)) {
		t.Fatal("negative compatibility control accepted changed meaning")
	}
}

func TestPresentationSeamHasIndependentKeysLocalesAndFailure(t *testing.T) {
	var catalog struct {
		Messages map[string]string `json:"messages"`
	}
	if err := json.Unmarshal(presentationResources, &catalog); err != nil {
		t.Fatal(err)
	}
	err := configcheck.NewValidation([]configcheck.Violation{{Field: "port", Rule: "range"}})
	identity := err.Code()
	original, _ := err.Violations()
	present := func(locale, key string, refuse bool) string {
		occurrence, ok := failure.Inspect(err)
		if !ok {
			t.Fatal("occurrence missing")
		}
		fallback := occurrence.Error()
		if refuse {
			return fallback
		}
		fields, complete := err.Violations()
		if !complete {
			return fallback
		}
		format, exists := catalog.Messages[locale+"/"+key]
		if !exists {
			return fallback
		}
		text := fmt.Sprintf(format, fields[0].Field)
		if len(text) > 128 {
			return fallback
		}
		fields[0].Field = "presenter-mutated"
		return text
	}
	outputs := map[string]bool{}
	for _, locale := range []string{"fixture-a", "fixture-b"} {
		for _, key := range []string{"cli.invalid", "report.warning"} {
			text := present(locale, key, false)
			if text == "" || text == string(identity) {
				t.Fatal("positive presentation fixture did not render")
			}
			outputs[text] = true
		}
	}
	if len(outputs) != 4 {
		t.Fatal("locale and resource identity were conflated")
	}
	delete(catalog.Messages, "fixture-a/cli.invalid")
	for _, text := range []string{present("missing", "missing", false), present("fixture-a", "cli.invalid", true), present("fixture-a", "cli.invalid", false)} {
		if text != string(identity) || len(text) > failure.MaxCodeBytes {
			t.Fatal("failed presenter has no safe fallback")
		}
	}
	after, complete := err.Violations()
	if err.Code() != identity || !complete || !reflect.DeepEqual(after, original) {
		t.Fatal("presentation changed machine facts")
	}
}

func FuzzValidationDetail(f *testing.F) {
	f.Add("port", "type", uint8(1))
	f.Add("enabled", "range", uint8(8))
	f.Add("private", "unsupported", uint8(9))
	f.Fuzz(func(t *testing.T, field, rule string, count uint8) {
		input := make([]configcheck.Violation, int(count))
		for index := range input {
			input[index] = configcheck.Violation{Field: field, Rule: rule}
		}
		err := configcheck.NewValidation(input)
		got, complete := err.Violations()
		expected := count > 0 && count <= configcheck.MaxViolations &&
			(field == "port" || field == "enabled") && (rule == "type" || rule == "range")
		if err.Code() != configcheck.InvalidInput || complete != expected ||
			complete && !reflect.DeepEqual(got, input) || !complete && len(got) != 0 {
			t.Fatal("required-detail contract violated")
		}
		if len(got) != 0 {
			got[0].Field = "mutation"
		}
		if fmt.Sprintf("%#v", err) != string(configcheck.InvalidInput) {
			t.Fatal("typed-detail formatting escaped core protection")
		}
	})
}
