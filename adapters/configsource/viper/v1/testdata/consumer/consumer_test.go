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
	"errors"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	prepare "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestPublicConfiguration(t *testing.T) {
	ctx := context.Background()
	runtime, err := adapters.New(ctx, adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(ctx)
	inbox, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	source, err := viper.New(viper.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := prepare.Prepare(ctx, prepare.Schema[viper.Settings]{Version: 1}, []prepare.Layer{{Kind: prepare.Base, Encoding: prepare.YAML, Content: []byte("encoding: json\ndefaults:\n- key: count\n  value: {kind: int64, text: '9007199254740993'}\n")}})
	if err != nil {
		t.Fatal(err)
	}
	options, err := declaration.ValueCopy()
	if err != nil {
		t.Fatal(err)
	}
	documents, err := source.Load(ctx, []viper.Input{{Settings: options, Reader: strings.NewReader(`{"text":"original","count":"42"}`)}})
	if err != nil {
		t.Fatal(err)
	}
	type model struct {
		Text  string `json:"text" mapstructure:"text"`
		Count int64  `json:"count" mapstructure:"count"`
	}
	value, err := viper.Decode[model](ctx, documents[0])
	if err != nil || value.Count != 42 {
		t.Fatal(value, err)
	}
	_, err = prepare.Prepare(ctx, prepare.Schema[model]{Version: 1}, []prepare.Layer{{Kind: prepare.Base, Encoding: prepare.JSON, Content: documents[0].RawCopy()}})
	if !errors.Is(err, prepare.ErrDecode) {
		t.Fatal("native weak decode replaced strict preparation")
	}
	if err := inbox.DeliverOne(ctx, func(_ context.Context, record adapters.Snapshot[viper.Evidence]) error { return record.Err() }); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "application.json")
	if err := os.WriteFile(path, []byte("{\"text\":\"raw\",\"count\":7}"), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := source.Source(viper.WatchSettings{Paths: []string{path}})
	if err != nil {
		t.Fatal(err)
	}
	var rawSource prepare.Source = files
	batch, _, err := rawSource.Capture(ctx)
	if err != nil || !batch.Valid() {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	observer, err := rawSource.Observe(wait)
	if err != nil {
		t.Fatal(err)
	}
	observation, err := observer.Next(wait)
	if err != nil || observation.Err != nil || observation.Batch.Len() != 1 {
		t.Fatal("public raw observation", err, observation.Err)
	}
	if err := observer.Close(wait); err != nil {
		t.Fatal(err)
	}
}
func TestOfflineAtlasAndPresentation(t *testing.T) {
	components := i18n.CoreComponents()
	for _, component := range []struct {
		name        string
		resources   fs.FS
		definitions []failure.Definition
	}{
		{"operation", adapters.Resources(), adapters.Definitions()},
		{"configuration_data", prepare.Resources(), prepare.Definitions()},
		{"configsource_viper", viper.Resources(), viper.Definitions()},
	} {
		components = append(components, i18n.Component{Module: "fathomry", Name: component.name, BaseLocale: "en", Resources: component.resources, Directory: "resources", Definitions: component.definitions})
	}
	catalog, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	type application struct {
		Application struct {
			I18n i18n.Preferences `json:"i18n"`
		} `json:"application"`
	}
	store := settings.NewStore[application]()
	initial := application{}
	initial.Application.I18n.Locale = "en"
	snapshot, err := settings.New(initial, func(value application) application { return value })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithSettings(store.Reader())
	if err != nil {
		t.Fatal(err)
	}
	marker := errors.New("private-native-canary")
	for _, definition := range append(prepare.Definitions(), viper.Definitions()...) {
		original, err := failure.New(definition, failure.Location{Operation: "consumer"}, marker)
		if err != nil {
			t.Fatal(err)
		}
		for _, locale := range []string{"en", "zh-CN"} {
			config := application{}
			config.Application.I18n.Locale = locale
			snapshot, _ := settings.New(config, func(value application) application { return value })
			if err := store.Publish(snapshot); err != nil {
				t.Fatal(err)
			}
			presented := presenter.Present(original).(*i18n.Presented)
			explained, found, err := catalog.Explain(definition.Code, locale)
			if err != nil || !found || presented.Issue() != nil || presented.Info().Message.Text != explained.Message.Text || !errors.Is(presented, marker) {
				t.Fatal("offline/log mismatch", err)
			}
			var output strings.Builder
			slog.New(slog.NewJSONHandler(&output, nil)).Error("configuration.failed", "error", presented)
			if strings.Contains(output.String(), "private-native-canary") || !strings.Contains(output.String(), locale) {
				t.Fatal("localized log privacy")
			}
		}
	}
}
