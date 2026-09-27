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
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	remote "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	local "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	c "github.com/frost-leo/fathomry/framework/configuration/v1"
)

type nested struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}
type project struct {
	Required string            `json:"required"`
	Count    int               `json:"count"`
	Large    uint64            `json:"large"`
	Enabled  bool              `json:"enabled"`
	Labels   map[string]string `json:"labels"`
	Items    []string          `json:"items"`
	Optional *nested           `json:"optional"`
	Text     string            `json:"text"`
}

func schema() c.Schema[project] {
	return c.Schema[project]{FormatVersion: 1, Defaults: c.DefaultSettings(project{
		Count: 7, Enabled: true, Labels: map[string]string{"default": "kept"}, Items: []string{"default"}, Optional: &nested{Name: "default", Count: 1}, Text: "default",
	})}
}
func file(t testing.TB, directory, name, body string) string {
	t.Helper()
	path := filepath.Join(directory, name)
	replace(t, path, body)
	return path
}
func replace(t testing.TB, path, body string) {
	t.Helper()
	temporary := path + ".next"
	if err := os.WriteFile(temporary, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, path); err != nil {
		t.Fatal(err)
	}
}
func selected(t testing.TB, name, path string, interval time.Duration) source.Selection {
	t.Helper()
	value, err := local.Select(local.Settings{Name: name, Documents: []local.File{{Name: "document", Path: path, Encoding: "yaml"}}, ReconcileInterval: interval})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func plan(value source.Selection, optional bool) c.Plan {
	return c.Plan{Modules: []adapters.Module{local.Module()}, Inputs: []c.Input{{Source: value, Documents: []c.LayerDocument{{Document: "document", Layer: c.Base, Optional: optional}}}}}
}
func await(t testing.TB, live *c.Live[project], predicate func(c.State[project]) bool) c.State[project] {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	state, err := live.Current()
	if err != nil {
		t.Fatal(err)
	}
	for !predicate(state) {
		state, err = live.Next(ctx, state.Cursor)
		if err != nil {
			t.Fatalf("live wait: %v", err)
		}
	}
	return state
}
func awaitRaw(t testing.TB, observer source.Observer, predicate func(source.State) bool) source.State {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	state, err := observer.Current()
	if err != nil {
		t.Fatal(err)
	}
	for !predicate(state) {
		state, err = observer.Next(ctx, state.Cursor)
		if err != nil {
			t.Fatalf("raw wait: %v", err)
		}
	}
	return state
}
func closeOwner(t testing.TB, close func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := close(ctx); err != nil {
		t.Error("cleanup failed", err)
	}
}
func TestLocalLoadFullContract(t *testing.T) {
	directory := t.TempDir()
	base := selected(t, "base", file(t, directory, "base.yaml", "format: 1\nproject: {required: supplied, count: 10, large: 18446744073709551615, labels: {base: yes}, items: [base], optional: {name: base}}"), 0)
	env := selected(t, "env", file(t, directory, "env.yaml", "format: 1\nproject: {count: 11, labels: {env: yes}, items: [env]}"), 0)
	override := selected(t, "local", file(t, directory, "local.yaml", "format: 1\nframework: {i18n: {default_locale: zh-CN}, time: {display_zone: Asia/Shanghai}}\nproject: {count: 12, enabled: false, items: [], optional: null, text: ''}"), 0)
	input := c.Plan{Modules: []adapters.Module{local.Module()}, Inputs: []c.Input{
		{Source: override, Documents: []c.LayerDocument{{Document: "document", Layer: c.Local}}},
		{Source: base, Documents: []c.LayerDocument{{Document: "document", Layer: c.Base}}},
		{Source: env, Documents: []c.LayerDocument{{Document: "document", Layer: c.Environment}}},
	}}
	t.Setenv("GH96_COUNT", "13")
	t.Setenv("GH96_TEXT", "null")
	input.Variables = []c.Variable{{Name: "GH96_COUNT", Field: "/project/count", Encoding: c.JSON}, {Name: "GH96_TEXT", Field: "/project/text", Required: true}}
	declared := schema()
	calls := 0
	declared.Validate = func(value c.Settings[project]) error {
		calls++
		if value.Project.Required == "" {
			return errors.New("required")
		}
		value.Project.Labels["default"] = "mutated"
		value.Framework.Time.DisplayZone = "Local"
		return nil
	}
	oldLocal := time.Local
	oldTZ := os.Getenv("TZ")
	snapshot, err := c.Load(context.Background(), declared, input)
	if err != nil {
		t.Fatal(err)
	}
	value, err := snapshot.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || value.Project.Count != 13 || value.Project.Large != math.MaxUint64 || value.Project.Enabled || len(value.Project.Items) != 0 || value.Project.Optional != nil || value.Project.Text != "null" || value.Project.Labels["default"] != "kept" || len(value.Project.Labels) != 3 {
		t.Fatal("precedence, exact values or validator isolation failed")
	}
	declared.Defaults.Project.Labels["default"] = "caller"
	value.Project.Labels["default"] = "returned"
	again, _ := snapshot.ValueCopy()
	if again.Project.Labels["default"] != "kept" {
		t.Fatal("snapshot alias")
	}
	presentation, err := snapshot.Presentation()
	if err != nil {
		t.Fatal(err)
	}
	locale, err := presentation.Locale()
	if err != nil || locale != "zh-CN" {
		t.Fatal("locale", err)
	}
	catalogs, err := c.Catalogs(local.Module())
	if err != nil {
		t.Fatal(err)
	}
	binding, err := catalogs.Bindings.Resolve(c.ModuleID+":invalid_value", locale)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := binding.Render(nil)
	if err != nil || rendered.Text == "" {
		t.Fatal("loaded locale not consumed", err)
	}
	stamp := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	display, err := presentation.FormatTime(stamp, time.RFC3339)
	if err != nil || display != "2026-01-02T11:04:05+08:00" || time.Local != oldLocal || os.Getenv("TZ") != oldTZ || stamp.Hour() != 3 {
		t.Fatal("display altered process/time semantics", err)
	}
	description, _ := snapshot.Description()
	if description.Revision == "" || description.FormatVersion != 1 || len(description.Sources) != 3 {
		t.Fatal("missing provenance")
	}
	for _, layer := range description.Provenance {
		for _, path := range layer.Fields {
			if strings.Contains(path, "GH96") || strings.Contains(path, "/labels/") {
				t.Fatal("dynamic provenance leaked")
			}
		}
	}
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			copy, err := snapshot.ValueCopy()
			if err != nil {
				t.Error(err)
			}
			copy.Project.Labels["default"] = "concurrent"
		})
	}
	workers.Wait()
	again, _ = snapshot.ValueCopy()
	if again.Project.Labels["default"] != "kept" {
		t.Fatal("concurrent copy shared data")
	}
}
func TestLocalPerLayerRejectionAndCoreGuard(t *testing.T) {
	directory := t.TempDir()
	path := file(t, directory, "input", "format: 1")
	base := selected(t, "one", path, 0)
	upper := selected(t, "two", file(t, directory, "upper", "format: 1\nproject: {count: 2}"), 0)
	input := plan(base, false)
	input.Inputs = append(input.Inputs, c.Input{Source: upper, Documents: []c.LayerDocument{{Document: "document", Layer: c.Local}}})
	for _, content := range []string{
		"", "   ", "[]", "project: {}", "format: 2", "format: 1.0", "format: 1e0", "format: '1'", "format: 1\nformat: 1",
		"format: 1\nunknown: true", "format: 1\nproject: {Count: 1}", "format: 1\nproject: {count: bad}", "format: 1\nproject: {count: 1.1}",
		"format: 1\nproject: {large: 18446744073709551616}", "format: 1\nproject: {count: null}", "format: 1\nproject: &x {count: 1}",
		"format: 1\nproject: ! {count: 1}", "format: 1\nproject: {count: 1, count: 2}", "format: 1\n---\nformat: 1",
	} {
		replace(t, path, content)
		if _, err := c.Load(context.Background(), schema(), input); err == nil {
			t.Fatalf("invalid lower layer accepted: %q", content)
		}
	}
	for _, core := range []string{
		"i18n: {default_locale: ''}", "i18n: {default_locale: '@@@'}", "time: {display_zone: ''}", "time: {display_zone: Local}",
		"time: {display_zone: ../etc}", "time: {display_zone: /etc}", "time: {display_zone: 'A/../B'}", "instance: {name: 'Not Valid'}",
	} {
		replace(t, path, "format: 1\nframework: {"+core+"}")
		declared := schema()
		calls := 0
		declared.Validate = func(value c.Settings[project]) error {
			calls++
			value.Framework = c.DefaultSettings(project{}).Framework
			return nil
		}
		if _, err := c.Load(context.Background(), declared, plan(base, false)); !errors.Is(err, c.ErrPreparation) || calls != 0 {
			t.Fatal("validator repaired original invalid Core", err, calls)
		}
	}
	replace(t, path, "format: 1\nframework: {time: {display_zone: Not/ARealZone}}")
	snapshot, err := c.Load(context.Background(), schema(), plan(base, false))
	if err != nil {
		t.Fatal("zone lookup ran in pure preparation", err)
	}
	if _, err := snapshot.Presentation(); !errors.Is(err, c.ErrPresentation) {
		t.Fatal("binding failure missing", err)
	}
	var zero c.Presentation
	var nilView *c.Presentation
	for _, view := range []*c.Presentation{&zero, nilView} {
		if _, err := view.Locale(); !errors.Is(err, c.ErrValue) {
			t.Fatal("invalid locale accessor", err)
		}
		if _, err := view.FormatTime(time.Time{}, ""); !errors.Is(err, c.ErrValue) {
			t.Fatal("invalid time accessor", err)
		}
	}
}
func TestPlainAdapterSettingsInsideProjectAndIncompleteDefaults(t *testing.T) {
	type bootstrap struct {
		Local    local.Settings  `json:"local"`
		Remote   remote.Settings `json:"remote"`
		Required string          `json:"required"`
	}
	directory := t.TempDir()
	path := file(t, directory, "input", "format: 1\nframework: {i18n: {default_locale: en}, time: {display_zone: UTC}}\nproject: {required: supplied}")
	defaults := c.Settings[bootstrap]{Project: bootstrap{Local: local.Settings{Name: "plain"}, Remote: remote.Settings{Password: "dto-secret-canary"}}}
	if data, err := json.Marshal(defaults); err != nil || !strings.Contains(string(data), "dto-secret-canary") {
		t.Fatal("plain DTO incorrectly guarded", err)
	}
	declared := c.Schema[bootstrap]{FormatVersion: 1, Defaults: defaults, Validate: func(value c.Settings[bootstrap]) error {
		if value.Project.Required == "" {
			return errors.New("required")
		}
		return nil
	}}
	snapshot, err := c.Load(context.Background(), declared, plan(selected(t, "source", path, 0), false))
	if err != nil {
		t.Fatal("incomplete defaults refused too early", err)
	}
	value, _ := snapshot.ValueCopy()
	if value.Project.Required != "supplied" || value.Project.Remote.Password != "dto-secret-canary" {
		t.Fatal("plain Adapter DTO did not survive")
	}
}
func TestEnvironmentAndDTOAdmission(t *testing.T) {
	t.Setenv("GH96_EMPTY", "")
	t.Setenv("GH96_JSON", "null")
	t.Setenv("GH96_BAD", "1.0")
	declared := schema()
	input := c.Plan{Variables: []c.Variable{{Name: "GH96_EMPTY", Field: "/project/text", Required: true}, {Name: "GH96_JSON", Field: "/project/optional", Encoding: c.JSON}}}
	snapshot, err := c.Load(context.Background(), declared, input)
	if err != nil {
		t.Fatal(err)
	}
	settings, _ := snapshot.ValueCopy()
	if settings.Project.Text != "" || settings.Project.Optional != nil {
		t.Fatal("empty/null environment semantics")
	}
	for _, variables := range [][]c.Variable{
		{{Name: "GH96_BAD", Field: "/project/count", Encoding: c.JSON}}, {{Name: "GH96_EMPTY", Field: "/format"}},
		{{Name: "GH96_EMPTY", Field: "/project/labels/arbitrary"}}, {{Name: "GH96_EMPTY", Field: "/project/items/0"}},
		{{Name: "GH96_EMPTY", Field: "/project/text"}, {Name: "GH96_EMPTY", Field: "/project/text"}},
		{{Name: "GH96_JSON", Field: "/project/optional", Encoding: c.JSON}, {Name: "GH96_EMPTY", Field: "/project/optional/name"}},
		{{Name: "INVALID-NAME", Field: "/project/text"}}, {{Name: "GH96_EMPTY", Field: "/project/text", Encoding: 9}},
	} {
		if _, err := c.Load(context.Background(), declared, c.Plan{Variables: variables}); err == nil {
			t.Fatal("invalid variable admission")
		}
	}
	if _, err := c.Load(context.Background(), c.Schema[map[string]string]{FormatVersion: 1}, c.Plan{}); !errors.Is(err, c.ErrSchema) {
		t.Fatal("map project root accepted", err)
	}
	type badTag struct {
		Value string `json:"value,omitempty"`
	}
	if _, err := c.Load(context.Background(), c.Schema[badTag]{FormatVersion: 1}, c.Plan{}); !errors.Is(err, c.ErrSchema) {
		t.Fatal("tag options accepted", err)
	}
	type recursive struct {
		Next *recursive `json:"next"`
	}
	if _, err := c.Load(context.Background(), c.Schema[recursive]{FormatVersion: 1}, c.Plan{}); !errors.Is(err, c.ErrSchema) {
		t.Fatal("recursive DTO accepted", err)
	}
	if _, err := c.Load(nil, declared, c.Plan{}); !errors.Is(err, c.ErrValue) {
		t.Fatal("nil context accepted", err)
	}
}

func TestExplicitLocalAndEnvironmentBootstrapThenRemoteLoad(t *testing.T) {
	fixture := newProtocolFixture(t, false)
	type bootstrap struct {
		Nacos remote.Settings `json:"nacos"`
	}
	document := struct {
		Format  uint32    `json:"format"`
		Project bootstrap `json:"project"`
	}{Format: 1, Project: bootstrap{Nacos: fixture.settings()}}
	raw, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	path := file(t, t.TempDir(), "bootstrap.json", string(raw))
	input := plan(selected(t, "bootstrap", path, 0), false)
	t.Setenv("GH96_BOOTSTRAP_NAME", "from-env")
	input.Variables = []c.Variable{{Name: "GH96_BOOTSTRAP_NAME", Field: "/project/nacos/name", Required: true}}
	loaded, err := c.Load(context.Background(), c.Schema[bootstrap]{FormatVersion: 1, Defaults: c.DefaultSettings(bootstrap{})}, input)
	if err != nil {
		t.Fatal("typed bootstrap loading failed", err)
	}
	value, err := loaded.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	if value.Project.Nacos.Name != "from-env" {
		t.Fatal("bootstrap environment binding lost")
	}
	selectedRemote, err := remote.Select(value.Project.Nacos)
	if err != nil {
		t.Fatal(err)
	}
	value.Project.Nacos.Servers[0].GRPCAddress = "mutated"
	snapshot, err := c.Load(context.Background(), schema(), remotePlan(selectedRemote))
	if err != nil {
		t.Fatal("remote load after bootstrap failed", err)
	}
	settings, _ := snapshot.ValueCopy()
	if settings.Project.Count != 21 {
		t.Fatal("bootstrap targeted wrong remote configuration")
	}
}
