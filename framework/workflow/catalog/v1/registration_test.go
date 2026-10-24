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

package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	declaration "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func authored(t testing.TB, input declaration.Declaration) Registration {
	t.Helper()
	bundle, err := declaration.Compile(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	request := Registration{Bundle: bundle, Resources: []Resource{{Target: Target{Kind: Definition}, Field: Name, Text: Text{English: "Server health", Context: "Published workflow name."}}}}
	seen := map[Target]bool{}
	for _, workflow := range input.Workflows {
		for _, mode := range workflow.Modes {
			target := Target{Kind: Mode, Version: workflow.Version, Mode: mode.Code}
			if seen[target] {
				continue
			}
			seen[target] = true
			request.Resources = append(request.Resources, Resource{Target: target, Field: Name, Text: Text{English: mode.Code}})
		}
	}
	return request
}
func registrationInput(t testing.TB) Registration { return authored(t, health.Declarations()) }

func TestPrepareRegistration(t *testing.T) {
	input := registrationInput(t)
	input.Category = &Category{Code: "operations", Name: &Text{English: "Operations"}, Description: &Text{English: ""}}
	input.Tags = map[string]string{"team": "observability", "region": ""}
	input.Resources = append(input.Resources,
		Resource{Target: Target{Kind: Definition}, Field: Description},
		Resource{Target: Target{Kind: Version, Version: "1"}, Field: Description, Text: Text{English: "One immutable implementation"}},
		Resource{Target: Target{Kind: ModeRevision, Version: "1", Mode: "standard", Revision: "1"}, Field: Description},
		Resource{Target: Target{Kind: Node, Version: "1", Node: "sample"}, Field: Name, Text: Text{English: "Sample"}},
		Resource{Target: Target{Kind: Activity, Version: "1", Node: "sample", Activity: "cpu"}, Field: Name, Text: Text{English: "Read CPU"}},
		Resource{Target: Target{Kind: Control, Version: "1", Control: "attempt_timeout"}, Field: Description, Text: Text{English: "Per-attempt limit"}},
	)
	prepared, err := Prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	info, err := prepared.Inspect()
	if err != nil || info.Namespace != "example" || info.Code != "server-health" || info.Versions != 1 || info.Modes != 2 || info.Revisions != 2 || info.Resources != 11 || info.MetadataBytes == 0 {
		t.Fatal("wrong admitted facts", info, err)
	}
	if len(prepared.state.resources) != 9 || len(prepared.state.initialResources) != 2 || prepared.state.category == nil || !prepared.state.category.declare || prepared.state.tags[0] != (tag{key: "region"}) {
		t.Fatal("initial metadata mixed into always-compared publication")
	}
	if prepared.state.initialResources[0].scope != "workflow.category" {
		t.Fatal("category resource owner differs")
	}
	foundEmpty := false
	for _, item := range prepared.state.resources {
		if item.scope == "workflow.definition" && strings.HasSuffix(item.key, "11:description") {
			if item.text.English != "" {
				t.Fatal("empty text changed")
			}
			foundEmpty = true
		}
	}
	if !foundEmpty {
		t.Fatal("intentional empty description was treated as absent")
	}

	reversed := input
	reversed.Resources = slices.Clone(input.Resources)
	slices.Reverse(reversed.Resources)
	second, err := Prepare(context.Background(), reversed)
	if err != nil || !reflect.DeepEqual(prepared.state, second.state) {
		t.Fatal("authoring order changed publication", err)
	}
	input.Resources[0].Text.English = "changed"
	input.Category.Code = "changed"
	input.Category.Name.English = "changed"
	input.Tags["team"] = "changed"
	if prepared.state.category.code != "operations" || prepared.state.tags[1].value != "observability" {
		t.Fatal("caller data retained")
	}
	for _, item := range prepared.state.resources {
		if item.text.English == "changed" {
			t.Fatal("resource text aliased")
		}
	}
	for _, item := range prepared.state.initialResources {
		if item.text.English == "changed" {
			t.Fatal("initial text aliased")
		}
	}
	copy := *prepared
	info.Code = "changed"
	independent, _ := copy.Inspect()
	if independent.Code != "server-health" {
		t.Fatal("inspection mutated snapshot")
	}
}

func TestPrepareRejectsIncorrectOwnership(t *testing.T) {
	for _, test := range []struct {
		name   string
		target Target
	}{
		{"unknown_kind", Target{Kind: "future"}},
		{"definition_has_version", Target{Kind: Definition, Version: "1"}},
		{"missing_version", Target{Kind: Version}},
		{"unselected_version", Target{Kind: Version, Version: "2"}},
		{"unselected_mode", Target{Kind: Mode, Version: "1", Mode: "missing"}},
		{"foreign_revision", Target{Kind: ModeRevision, Version: "1", Mode: "standard", Revision: "2"}},
		{"node", Target{Kind: Node, Version: "1", Node: "missing"}},
		{"wrong_activity_parent", Target{Kind: Activity, Version: "1", Node: "summary", Activity: "cpu"}},
		{"binding_is_not_activity", Target{Kind: Activity, Version: "1", Node: "sample", Activity: "read_cpu"}},
		{"control", Target{Kind: Control, Version: "1", Control: "missing"}},
		{"unused_selector", Target{Kind: Control, Version: "1", Control: "attempt_timeout", Node: "sample"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			input := registrationInput(t)
			input.Resources = append(input.Resources, Resource{Target: test.target, Field: Name, Text: Text{English: "valid"}})
			result, err := Prepare(context.Background(), input)
			diagnostic, ok := Diagnose(err)
			if result != nil || !errors.Is(err, ErrMetadata) || !ok || diagnostic.Reason != "owner" || diagnostic.Path != "/resources/3" {
				t.Fatal("invalid owner accepted or unsafe diagnostic", err)
			}
		})
	}
	for _, index := range []int{0, 1, 2} {
		input := registrationInput(t)
		input.Resources = append(input.Resources[:index:index], input.Resources[index+1:]...)
		if value, err := Prepare(context.Background(), input); value != nil || !errors.Is(err, ErrMetadata) {
			t.Fatal("required name omitted", index, err)
		}
	}
	input := registrationInput(t)
	input.Resources = append(input.Resources, input.Resources[0])
	if value, err := Prepare(context.Background(), input); value != nil || !errors.Is(err, ErrMetadata) {
		t.Fatal("duplicate resource accepted", err)
	}
	input = registrationInput(t)
	input.Resources[0].Field = "enabled"
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrMetadata) {
		t.Fatal("management field accepted as display metadata")
	}
}

func TestPrepareCategoryAndTags(t *testing.T) {
	input := registrationInput(t)
	input.Category = &Category{Code: "existing-category"}
	prepared, err := Prepare(context.Background(), input)
	if err != nil || prepared.state.category.declare || len(prepared.state.initialResources) != 0 {
		t.Fatal("reference invented category resources", err)
	}
	for _, category := range []*Category{
		{}, {Code: "x", Description: &Text{}}, {Code: "x", Name: &Text{}},
		{Code: strings.Repeat("x", 129)}, {Code: "nul\x00"},
	} {
		input.Category = category
		if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrMetadata) {
			t.Fatal("malformed initial category admitted", err)
		}
	}
	input.Category = nil
	for _, key := range []string{"", "id", "ENABLED", "row_revision", "default_version_id", "default_mode_id", "default_mode_version_id", "fathomry.internal", "Temporal.Namespace", "bad\x00", strings.Repeat("x", 129)} {
		input.Tags = map[string]string{key: "value"}
		if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrMetadata) {
			t.Fatal("reserved/invalid tag admitted", err)
		}
	}
	input.Tags = map[string]string{"raw:标签": strings.Repeat("x", MaxTagValueBytes)}
	if _, err := Prepare(context.Background(), input); err != nil {
		t.Fatal("bounded raw tag rejected", err)
	}
	input.Tags["raw:标签"] += "x"
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrMetadata) {
		t.Fatal("oversized tag admitted")
	}
	input.Tags = map[string]string{}
	for index := 0; index < MaxTags+1; index++ {
		input.Tags["tag"+strconv.Itoa(index)] = ""
	}
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrLimit) {
		t.Fatal("tag count unbounded")
	}
}

func TestPrepareTextAndAggregateLimits(t *testing.T) {
	input := registrationInput(t)
	input.Resources[0].Text = Text{English: strings.Repeat("界", MaxNameCharacters), Context: strings.Repeat("x", MaxContextBytes)}
	if _, err := Prepare(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []Text{
		{}, {English: " \n\t"}, {English: strings.Repeat("界", MaxNameCharacters+1)},
		{English: "valid", Context: strings.Repeat("x", MaxContextBytes+1)},
		{English: "bad\x00"}, {English: string([]byte{0xff})},
	} {
		input.Resources[0].Text = invalid
		if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrMetadata) {
			t.Fatal("invalid text admitted", err)
		}
	}
	input = registrationInput(t)
	input.Resources = append(input.Resources, Resource{Target: Target{Kind: Definition}, Field: Description, Text: Text{English: strings.Repeat("x", MaxTextBytes)}})
	if _, err := Prepare(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	input.Resources[3].Text.English += "x"
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrMetadata) {
		t.Fatal("text byte cap ignored")
	}
	input.Resources = make([]Resource, MaxResources+1)
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrLimit) {
		t.Fatal("resource count unbounded")
	}

	authoredInput := declaration.Declaration{}
	for index := 0; index < 64; index++ {
		workflow := health.Declarations().Workflows[0]
		workflow.Version = strconv.Itoa(index)
		workflow.Modes = append(workflow.Modes, declaration.ModeDeclaration{Code: "third", Revision: "1"}, declaration.ModeDeclaration{Code: "fourth", Revision: "1"})
		authoredInput.Workflows = append(authoredInput.Workflows, workflow)
	}
	input = authored(t, authoredInput)
	for _, workflow := range authoredInput.Workflows {
		input.Resources = append(input.Resources, Resource{Target: Target{Kind: Version, Version: workflow.Version}, Field: Description, Text: Text{English: strings.Repeat("x", MaxTextBytes)}})
		for _, mode := range workflow.Modes {
			input.Resources = append(input.Resources, Resource{Target: Target{Kind: Mode, Version: workflow.Version, Mode: mode.Code}, Field: Description, Text: Text{English: strings.Repeat("x", MaxTextBytes)}})
		}
	}
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrLimit) {
		t.Fatal("valid per-text limits bypassed aggregate metadata bound", err)
	}
	input.Resources = input.Resources[:257]
	prepared, err := Prepare(context.Background(), input)
	if err != nil {
		t.Fatal("metadata bound shrank declaration profile", err)
	}
	info, _ := prepared.Inspect()
	if info.Versions != 64 || info.Revisions != 256 || info.Modes != 256 {
		t.Fatal("full entry counts changed", info)
	}
}

func TestResourceKeyEncoding(t *testing.T) {
	key, err := resourceKey("a", "bc")
	if err != nil || key != "v1;1:a2:bc" {
		t.Fatal("encoding changed", key, err)
	}
	other, _ := resourceKey("ab", "c")
	if other == key {
		t.Fatal("ambiguous resource identity")
	}
	unicodeKey, _ := resourceKey("界", "name")
	if unicodeKey != "v1;3:界4:name" {
		t.Fatal("length is not UTF-8 bytes", unicodeKey)
	}
	largest, err := resourceKey(strings.Repeat("界", 85), strings.Repeat("a", 128), strings.Repeat("v", 128), strings.Repeat("m", 128), strings.Repeat("r", 128), "description")
	if err != nil || len(largest) > 1024 {
		t.Fatal("maximal admitted tuple does not fit", len(largest), err)
	}
	if _, err := resourceKey(strings.Repeat("x", 1024)); !errors.Is(err, ErrLimit) {
		t.Fatal("framing escaped key cap")
	}
	for _, pair := range [][2]string{{"a", "A"}, {"é", "e\u0301"}, {"x", "x "}} {
		first, _ := resourceKey(pair[0])
		second, _ := resourceKey(pair[1])
		if first == second {
			t.Fatal("machine identity normalized")
		}
	}
}

func TestPreparationHandlesCancellationAndDiagnostics(t *testing.T) {
	input := registrationInput(t)
	if _, err := Prepare(nil, input); !errors.Is(err, ErrInput) {
		t.Fatal("nil context accepted")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("private-cancellation-canary")
	cancel(cause)
	if _, err := Prepare(ctx, input); !errors.Is(err, ErrCanceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation cause lost", err)
	}
	for _, bundle := range []*declaration.Bundle{nil, new(declaration.Bundle)} {
		input.Bundle = bundle
		if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrInput) || !errors.Is(err, declaration.ErrHandle) {
			t.Fatal("unverified bundle accepted", err)
		}
	}
	empty, err := declaration.Compile(context.Background(), declaration.Declaration{})
	if !errors.Is(err, declaration.ErrInvalid) || empty != nil {
		t.Fatal("declaration authority admitted an empty selection", err)
	}
	input.Bundle = empty
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrInput) {
		t.Fatal("empty aggregate accepted")
	}
	mixed := health.Declarations()
	other := health.Declarations().Workflows[0]
	other.Namespace = "other"
	mixed.Workflows = append(mixed.Workflows, other)
	input = authored(t, mixed)
	if _, err := Prepare(context.Background(), input); !errors.Is(err, ErrInput) {
		t.Fatal("cross-workflow transaction admitted")
	}
	for _, zero := range []*PreparedRegistration{nil, new(PreparedRegistration)} {
		if _, err := zero.Inspect(); !errors.Is(err, ErrHandle) {
			t.Fatal("invalid prepared handle accepted")
		}
	}
	input = registrationInput(t)
	input.Resources[0].Text.English = "private-payload-canary"
	prepared, err := Prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{input, &input, prepared, *prepared} {
		if strings.Contains(fmt.Sprintf("%+v %#v", value, value), "private-payload") {
			t.Fatal("formatted payload")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("publication", "value", value)
		if strings.Contains(output.String(), "private-payload") {
			t.Fatal("logged payload")
		}
	}
	if _, err := json.Marshal(prepared); !errors.Is(err, ErrSerialization) {
		t.Fatal("prepared state serialized", err)
	}
	if _, ok := Diagnose(errors.Join(ErrMetadata)); ok {
		t.Fatal("foreign error claimed as a direct diagnostic")
	}
	messages, err := i18n.Prepare(Component())
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(messages)
	if err != nil {
		t.Fatal(err)
	}
	for _, locale := range []string{"en", "zh-CN"} {
		selected, err := presenter.WithLocale(locale)
		if err != nil {
			t.Fatal(err)
		}
		for _, definition := range Definitions() {
			shown, err := selected.Render(string(definition.Identifier), nil, nil)
			if err != nil || shown.Locale != locale || shown.Text == "" {
				t.Fatal("missing localization", locale, err)
			}
		}
	}
}

func TestPreparedConcurrentInspection(t *testing.T) {
	prepared, err := Prepare(context.Background(), registrationInput(t))
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 16 {
		workers.Go(func() {
			for range 100 {
				info, err := prepared.Inspect()
				if err != nil || info.Code != "server-health" {
					t.Error("snapshot changed", err)
				}
			}
		})
	}
	workers.Wait()
}

func FuzzResourceKeyInjectivity(f *testing.F) {
	f.Add("a", "bc", "ab", "c")
	f.Add("界", "name", "界", "description")
	f.Add("v1;3:a:b", "1:c", "v1;3:a", "b1:c")
	f.Fuzz(func(t *testing.T, first, second, third, fourth string) {
		if len(first)+len(second)+len(third)+len(fourth) > 4096 {
			return
		}
		left, errLeft := resourceKey(first, second)
		right, errRight := resourceKey(third, fourth)
		if errLeft != nil || errRight != nil {
			return
		}
		if len(left) > MaxResourceKeyBytes || len(right) > MaxResourceKeyBytes {
			t.Fatal("key bound bypassed")
		}
		if (left == right) != (first == third && second == fourth) {
			t.Fatal("noninjective key encoding")
		}
	})
}

func TestModeRevisionAndOptionalMetadataPresence(t *testing.T) {
	declared := health.Declarations()
	declared.Workflows[0].Modes = append(declared.Workflows[0].Modes, declaration.ModeDeclaration{Code: "standard", Revision: "2"})
	input := authored(t, declared)
	prepared, err := Prepare(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := prepared.Inspect()
	if info.Modes != 2 || info.Revisions != 3 || info.Resources != 3 {
		t.Fatal("mode names coupled to revision count", info)
	}
	withDescription := input
	withDescription.Resources = append(slices.Clone(input.Resources), Resource{Target: Target{Kind: Definition}, Field: Description})
	described, err := Prepare(context.Background(), withDescription)
	if err != nil || len(described.state.resources) != len(prepared.state.resources)+1 {
		t.Fatal("empty and absent description collapsed", err)
	}
	if len(prepared.state.resources) != 3 {
		t.Fatal("another preparation mutated the prior snapshot")
	}
}

func TestMetadataBoundAtExactByteLimit(t *testing.T) {
	total := MaxMetadataBytes - 1
	if err := charge(&total, 1); err != nil || total != MaxMetadataBytes {
		t.Fatal("exact bound rejected", err)
	}
	if err := charge(&total, 1); !errors.Is(err, ErrLimit) || total != MaxMetadataBytes {
		t.Fatal("overflow changed accounting", err)
	}
}

func TestDiagnosticsRetainOnlySafePaths(t *testing.T) {
	input := registrationInput(t)
	input.Resources[0].Text.English = "private-payload-canary"
	input.Resources = append(input.Resources, Resource{Target: Target{Kind: Node, Version: "1", Node: "private-node-canary"}, Field: Name, Text: Text{English: "private-text-canary"}})
	_, err := Prepare(context.Background(), input)
	diagnostic, ok := Diagnose(err)
	if !ok || diagnostic.Code != ErrMetadata || diagnostic.Path != "/resources/3" {
		t.Fatal("missing scoped diagnostic", err)
	}
	raw, problem := json.Marshal(diagnostic)
	if problem != nil || bytes.Contains(raw, []byte("canary")) || strings.Contains(err.Error(), "canary") {
		t.Fatal("payload in error")
	}
	diagnostic.Path = "changed"
	again, _ := Diagnose(err)
	if again.Path != "/resources/3" {
		t.Fatal("diagnostic alias")
	}
}
