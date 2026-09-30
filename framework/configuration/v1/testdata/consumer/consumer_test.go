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

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
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
	runtime, err := framework.New(ctx, framework.Options{Operations: adapters.Options{MaxWorkBytes: 128 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	sourceEvidence, _ := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{})
	sourceReceiver, err := framework.StartReceiver(context.Background(), sourceEvidence, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[viper.Evidence]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer sourceReceiver.Close(context.Background())
	configEvidence, _ := adapters.NewInbox[configuration.Evidence](adapters.EvidenceOptions{})
	configReceiver, err := framework.StartReceiver(context.Background(), configEvidence, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[configuration.Evidence]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer configReceiver.Close(context.Background())
	client, err := viper.New(viper.Dependencies{Runtime: runtime.Operations(), Evidence: sourceEvidence})
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	appPath, businessPath := filepath.Join(directory, "application.yaml"), filepath.Join(directory, "business.yaml")
	if err := os.WriteFile(appPath, []byte("application: {i18n: {locale: zh-CN}}\ncustom: {name: project, roles: {primary: selected}}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(businessPath, []byte("access: first\nsecret: secret-first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	appSource, _ := client.Source(viper.WatchSettings{Paths: []string{appPath}, Interval: 10 * time.Millisecond})
	businessSource, _ := client.Source(viper.WatchSettings{Paths: []string{businessPath}, Interval: 10 * time.Millisecond})
	dependencies := configuration.Dependencies{Runtime: runtime.Operations(), Evidence: configEvidence}
	application, err := configuration.Watch(ctx, configuration.Declaration[project]{
		Schema: configsource.Schema[project]{Version: 1, Validate: func(_ context.Context, value project) error {
			if value.Custom.Name == "" || value.Custom.Roles["primary"] == "" {
				return errors.New("invalid project extension")
			}
			return value.Application.I18n.Validate()
		}},
		Source: appSource, Layers: []configuration.Layer{{Kind: configsource.Base, Encoding: configsource.YAML}},
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
	business, err := configuration.Watch(ctx, configuration.Declaration[bundle]{
		Schema: configsource.Schema[bundle]{Version: 1, Validate: func(_ context.Context, value bundle) error {
			if value.Access == "" || value.Secret != "secret-"+value.Access {
				return errors.New("invalid bundle")
			}
			return nil
		}},
		Source: businessSource, Layers: []configuration.Layer{{Kind: configsource.Base, Encoding: configsource.YAML}},
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
	_, failure := configuration.Load(ctx, configuration.Declaration[project]{Schema: configsource.Schema[project]{Version: 1}, Layers: []configuration.Layer{{Kind: configsource.Base, Encoding: configsource.JSON}}}, dependencies)
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
	if err := runtime.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := configReceiver.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sourceReceiver.Finish(ctx); err != nil {
		t.Fatal(err)
	}
}
