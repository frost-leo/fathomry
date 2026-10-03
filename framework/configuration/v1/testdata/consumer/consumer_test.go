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
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type project struct {
	Application struct {
		I18n i18n.Preferences `json:"i18n"`
	} `json:"application"`
	Custom struct {
		Name  string            `json:"name"`
		Roles map[string]string `json:"roles"`
	} `json:"custom"`
}
type bundle struct {
	Access string `json:"access"`
	Secret string `json:"secret"`
}

func TestIndependentConfigurationDomains(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	directory := t.TempDir()
	appPath, businessPath := filepath.Join(directory, "application.yaml"), filepath.Join(directory, "business.yaml")
	if err := os.WriteFile(appPath, []byte("application: {i18n: {locale: zh-CN}}\ncustom: {name: project, roles: {primary: selected}}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(businessPath, []byte("access: first\nsecret: secret-first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	appSource, _ := configuration.Viper(configuration.ViperOptions{Documents: []configuration.File{{Path: appPath, Kind: configuration.Base, Encoding: configuration.YAML}}, Interval: 10 * time.Millisecond})
	businessSource, _ := configuration.Viper(configuration.ViperOptions{Documents: []configuration.File{{Path: businessPath, Kind: configuration.Base, Encoding: configuration.YAML}}, Interval: 10 * time.Millisecond})
	dependencies := configuration.Dependencies{Provider: appSource}
	application, err := configuration.Watch(ctx, configuration.Declaration[project]{
		Schema: configuration.Schema[project]{Version: 1, Validate: func(_ context.Context, value project) error {
			if value.Custom.Name == "" || value.Custom.Roles["primary"] == "" {
				return errors.New("invalid project extension")
			}
			return value.Application.I18n.Validate()
		}},
	}, dependencies, configuration.WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close(context.Background())
	event, err := application.Next(ctx)
	if err != nil || !event.Accepted {
		t.Fatal("initial project not accepted", err)
	}
	if err := settings.Configure(application.Reader()); err != nil {
		t.Fatal(err)
	}
	before, err := application.Capture()
	if err != nil {
		t.Fatal(err)
	}
	defaultView, err := settings.Default()
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := settings.Read(defaultView, "/custom/name", func(value string) string { return value })
	if err != nil || !ok || value != "project" {
		t.Fatal("default reader lost root extension")
	}
	dependencies.Provider = businessSource
	business, err := configuration.Watch(ctx, configuration.Declaration[bundle]{
		Schema: configuration.Schema[bundle]{Version: 1, Validate: func(_ context.Context, value bundle) error {
			if value.Access == "" || value.Secret != "secret-"+value.Access {
				return errors.New("invalid bundle")
			}
			return nil
		}},
	}, dependencies, configuration.WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer business.Close(context.Background())
	if event, err := business.Next(ctx); err != nil || !event.Accepted {
		t.Fatal(err)
	}
	if err := os.WriteFile(businessPath, []byte("access: second\nsecret: secret-second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for {
		event, err := business.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if event.Accepted {
			break
		}
	}
	after, _ := application.Capture()
	if before.Description().Revision != after.Description().Revision {
		t.Fatal("business update republished application")
	}
	latest, _ := business.Capture()
	credentials, _ := latest.ValueCopy()
	if credentials.Access != "second" || credentials.Secret != "secret-second" {
		t.Fatal("bundle not atomic")
	}
	catalog, err := i18n.Prepare(configuration.Components()...)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithSettings(application.Reader())
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	logger := framework.NewErrorLog(slog.New(slog.NewJSONHandler(&output, nil)), presenter)
	dependencies.Provider = configuration.Provider{}
	_, failure := configuration.Load(ctx, configuration.Declaration[project]{Schema: configuration.Schema[project]{Version: 1}}, dependencies)
	emitted := logger.Emit(ctx, failure)
	if emitted.Issue != nil || !errors.Is(emitted.Presented, configuration.ErrDeclaration) || !strings.Contains(output.String(), "zh-CN") {
		t.Fatal("configured log boundary lost code/locale")
	}
	for _, definition := range configuration.Definitions() {
		if _, found, err := catalog.Explain(definition.Code, "en"); err != nil || !found {
			t.Fatal("missing offline definition")
		}
	}
	if err := application.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := business.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if records, ready := application.Records(); !ready || len(records) != 2 {
		t.Fatal("application evidence ownership incomplete")
	}
	if records, ready := business.Records(); !ready || len(records) != 2 {
		t.Fatal("business evidence ownership incomplete")
	}
}
