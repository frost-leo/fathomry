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

package i18n_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/i18n"
)

func sourceRevision(t testing.TB, catalog *i18n.Catalog, id string) string {
	t.Helper()
	for _, source := range catalog.Snapshot().Sources {
		if source.ID == id {
			return source.SHA256
		}
	}
	t.Fatal("source revision not found")
	return ""
}

func TestSourceFreshnessCoversContextParametersAndEveryVariant(t *testing.T) {
	inputs := resources(t)
	old := prepared(t, inputs)
	edits := fixture[adversarialCases](t, "adversarial.json").SourceEdits
	tests := []struct {
		name, id string
		edit     func(map[string]any)
		args     i18n.Arguments
	}{
		{"wording", "demo.welcome", func(value map[string]any) { value["forms"].(map[string]any)["other"] = edits["wording"] }, i18n.Arguments{"Name": ""}},
		{"description", "demo.welcome", func(value map[string]any) { value["description"] = edits["description"] }, i18n.Arguments{"Name": ""}},
		{"parameter-description", "demo.welcome", func(value map[string]any) {
			value["parameters"].(map[string]any)["Name"].(map[string]any)["description"] = edits["parameterDescription"]
		}, i18n.Arguments{"Name": ""}},
		{"parameter-type", "demo.welcome", func(value map[string]any) {
			value["parameters"].(map[string]any)["Name"].(map[string]any)["type"] = edits["changedType"]
		}, i18n.Arguments{"Name": int64(1)}},
		{"singular-only", "demo.files", func(value map[string]any) { value["forms"].(map[string]any)["one"] = edits["singular"] }, i18n.Arguments{"Owner": "", "Count": i18n.Number("1")}},
		{"parameter-rename", "demo.welcome", func(value map[string]any) {
			params := value["parameters"].(map[string]any)
			params["Recipient"] = params["Name"]
			delete(params, "Name")
			forms := value["forms"].(map[string]any)
			forms["other"] = strings.ReplaceAll(forms["other"].(string), ".Name", ".Recipient")
		}, i18n.Arguments{"Recipient": ""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := editLocale(t, inputs, "en", func(value map[string]any) { test.edit(message(value, test.id)) })
			current := prepared(t, changed)
			if sourceRevision(t, old, test.id) == sourceRevision(t, current, test.id) {
				t.Fatal("changed contract kept source fingerprint")
			}
			result, err := current.Render("zh-Hans", test.id, test.args)
			english, englishErr := current.Render("en", test.id, test.args)
			if err != nil || englishErr != nil || result.Text != english.Text || result.Fallback != i18n.StaleTranslation ||
				result.ResourceLocale != "en" {
				t.Fatal("stale translation used", result, err)
			}
			snapshot := current.Snapshot()
			if len(snapshot.Stale) == 0 {
				t.Fatal("stale metadata missing")
			}
			snapshot.Stale[0] = i18n.StaleEntry{}
			if reflect.DeepEqual(snapshot, current.Snapshot()) {
				t.Fatal("stale metadata aliases")
			}
		})
	}
	changed := editLocale(t, inputs, "en", func(value map[string]any) {
		message(value, "demo.welcome")["forms"].(map[string]any)["other"] = edits["wording"]
	})
	current := prepared(t, changed)
	fresh := sourceRevision(t, current, "demo.welcome")
	reviewed := editLocale(t, changed, "zh-Hans", func(value map[string]any) { message(value, "demo.welcome")["source"] = fresh })
	result, err := prepared(t, reviewed).Render("zh-Hans", "demo.welcome", i18n.Arguments{"Name": ""})
	if err != nil || result.Fallback != i18n.NoFallback || result.ResourceLocale != "zh-Hans" {
		t.Fatal("explicit rereview did not restore translation", err)
	}
}

func TestConsumerResourceCompatibilityAndRollback(t *testing.T) {
	original := resources(t)
	old := prepared(t, original)
	oldArgs := i18n.Arguments{"Name": ""}
	baseline, err := old.Render("en", "demo.welcome", oldArgs)
	if err != nil {
		t.Fatal(err)
	}
	added := editLocale(t, original, "en", func(value map[string]any) {
		old := message(value, "demo.welcome")
		addition := make(map[string]any, len(old))
		for name, item := range old {
			addition[name] = item
		}
		addition["id"] = "business.new-message"
		value["messages"] = append(value["messages"].([]any), addition)
	})
	newCatalog := prepared(t, added)
	oldOnNew, err := newCatalog.Render("en", "demo.welcome", oldArgs)
	if err != nil || oldOnNew.Text != baseline.Text {
		t.Fatal("additive upgrade broke old consumer")
	}
	_, err = old.Render("en", "business.new-message", oldArgs)
	requireCode(t, err, i18n.MessageNotFound)
	addedResult, err := newCatalog.Render("zh-Hans", "business.new-message", oldArgs)
	if err != nil || addedResult.Fallback != i18n.MissingTranslation {
		t.Fatal("new message fallback failed")
	}
	breaking := editLocale(t, original, "en", func(value map[string]any) {
		params := message(value, "demo.welcome")["parameters"].(map[string]any)
		params["Required"] = params["Name"]
	})
	incompatible := prepared(t, breaking)
	_, err = incompatible.Render("en", "demo.welcome", oldArgs)
	requireCode(t, err, i18n.InvalidArguments)
	newArgs := i18n.Arguments{"Name": "", "Required": ""}
	_, err = old.Render("en", "demo.welcome", newArgs)
	requireCode(t, err, i18n.InvalidArguments)
	if _, err = incompatible.Render("en", "demo.welcome", newArgs); err != nil {
		t.Fatal(err)
	}
	rolledBack := prepared(t, original)
	back, err := rolledBack.Render("en", "demo.welcome", oldArgs)
	if err != nil || back != baseline || rolledBack.Snapshot().ID != old.Snapshot().ID {
		t.Fatal("compatible rollback changed identity")
	}
	unknown := editLocale(t, added, "en", func(value map[string]any) { value["profile"] = "fathomry.i18n/v99" })
	replacement, err := i18n.Prepare(unknown)
	requireCode(t, err, i18n.UnsupportedProfile)
	if replacement != nil {
		t.Fatal("unsupported replacement published")
	}
	after, err := newCatalog.Render("en", "demo.welcome", oldArgs)
	if err != nil || after != oldOnNew {
		t.Fatal("failed preparation mutated existing snapshot")
	}
}

func TestTranslationCorrectionsAndSnapshotProvenance(t *testing.T) {
	inputs := resources(t)
	old := prepared(t, inputs)
	changed := editLocale(t, inputs, "zh-Hans", func(value map[string]any) {
		message(value, "demo.welcome")["forms"].(map[string]any)["other"] = fixture[adversarialCases](t, "adversarial.json").TranslationFix
	})
	newCatalog := prepared(t, changed)
	if newCatalog.Snapshot().ID == old.Snapshot().ID || !reflect.DeepEqual(newCatalog.Snapshot().Sources, old.Snapshot().Sources) {
		t.Fatal("wording correction changed wrong version axis")
	}
	before, err := old.Render("zh-Hans", "demo.welcome", i18n.Arguments{"Name": ""})
	if err != nil {
		t.Fatal(err)
	}
	after, err := newCatalog.Render("zh-Hans", "demo.welcome", i18n.Arguments{"Name": ""})
	if err != nil || before.Text == after.Text || after.Fallback != i18n.NoFallback {
		t.Fatal("current translation correction not usable")
	}
	whitespace := slices.Clone(inputs)
	whitespace[0].Data = append(slices.Clone(whitespace[0].Data), '\n')
	repacked := prepared(t, whitespace)
	if repacked.Snapshot().ID == old.Snapshot().ID || !reflect.DeepEqual(repacked.Snapshot().Sources, old.Snapshot().Sources) {
		t.Fatal("raw packaging and source revision conflated")
	}
	renamed := slices.Clone(inputs)
	renamed[0].Name = "renamed.json"
	if prepared(t, renamed).Snapshot().ID == old.Snapshot().ID {
		t.Fatal("resource names omitted from composition identity")
	}
	bad := editLocale(t, inputs, "zh-Hans", func(value map[string]any) {
		forms := message(value, "demo.welcome")["forms"].(map[string]any)
		forms["other"] = strings.ReplaceAll(forms["other"].(string), ".Name", ".Undeclared")
	})
	_, err = i18n.Prepare(bad)
	requireCode(t, err, i18n.InvalidTemplate)
}
