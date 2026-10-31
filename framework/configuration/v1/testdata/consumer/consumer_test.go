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
	"os"
	"path/filepath"
	"testing"
	"time"

	"example.org/configuration-consumer/setting"
	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestIndependentProject(t *testing.T) {
	if os.Getenv("FATHOMRY_TEST_NACOS_HTTP") == "" || os.Getenv("FATHOMRY_TEST_NACOS_GRPC") == "" {
		t.Fatal("run through the parent protocol-peer acceptance test")
	}
	catalogs, err := framework.Prepare(append(i18n.CoreComponents(), configuration.Component())...)
	if err != nil || catalogs.Errors == nil || catalogs.Messages == nil {
		t.Fatal(err)
	}
	for _, remote := range []bool{false, true} {
		t.Run(map[bool]string{false: "development_local", true: "production_remote"}[remote], func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			runtime, err := adapters.New(ctx, adapters.Options{})
			if err != nil {
				t.Fatal(err)
			}
			localEvidence, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{})
			if err != nil {
				t.Fatal(err)
			}
			remoteEvidence, err := adapters.NewInbox[nacos.Evidence](adapters.EvidenceOptions{})
			if err != nil {
				t.Fatal(err)
			}
			dependencies := configuration.Dependencies{Local: viper.Dependencies{Runtime: runtime, Evidence: localEvidence}}
			dependencies.Remote = nacos.Dependencies{Runtime: runtime, Evidence: remoteEvidence}
			dir := t.TempDir()
			if err := os.MkdirAll(filepath.Join(dir, "config/environment"), 0700); err != nil {
				t.Fatal(err)
			}
			app := filepath.Join(dir, "config/base.yaml")
			if err := os.WriteFile(app, []byte("application:\n workers: 2\n"), 0600); err != nil {
				t.Fatal(err)
			}
			environment := "development"
			bootstrap := "FATHOMRY_ENV=development\nFATHOMRY_CONFIG_MODE=local\nFATHOMRY_CONFIG_PROVIDER=viper\n"
			if err := os.WriteFile(filepath.Join(dir, "config/environment/development.yaml"), []byte("{}"), 0600); err != nil {
				t.Fatal(err)
			}
			if remote {
				environment = "production"
				remoteSettings := nacos.Settings{Name: "consumer", Servers: []nacos.Server{{HTTPURL: os.Getenv("FATHOMRY_TEST_NACOS_HTTP"), GRPCAddress: os.Getenv("FATHOMRY_TEST_NACOS_GRPC")}},
					Keys: []nacos.Key{{DataID: "main"}}, AllowInsecure: true, RequestTimeout: time.Second}
				raw, err := json.Marshal(remoteSettings)
				if err != nil {
					t.Fatal(err)
				}
				documents, err := json.Marshal([]configuration.Document{{Kind: "base", Encoding: configuration.YAML}})
				if err != nil {
					t.Fatal(err)
				}
				bootstrap = "FATHOMRY_ENV=production\nFATHOMRY_CONFIG_MODE=remote\nFATHOMRY_CONFIG_PROVIDER=nacos\n" +
					"FATHOMRY_CONFIG_DOCUMENTS_JSON='" + string(documents) + "'\nFATHOMRY_NACOS_SETTINGS_JSON='" + string(raw) + "'\n"
			}
			path := filepath.Join(dir, "bootstrap-"+environment+".env")
			if err := os.WriteFile(path, []byte(bootstrap), 0600); err != nil {
				t.Fatal(err)
			}
			selected, err := configuration.LoadBootstrap(ctx, configuration.BootstrapInput{Path: path}, dependencies.Local)
			if err != nil {
				t.Fatal(err)
			}
			delivery, err := localEvidence.NextReleased(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := delivery.Ack(); err != nil {
				t.Fatal(err)
			}
			session, err := configuration.LoadConfiguration(ctx, selected, setting.Schema(), nil, dependencies)
			t.Cleanup(func() {
				finish, stop := context.WithTimeout(context.Background(), 3*time.Second)
				defer stop()
				if session != nil {
					if err := session.Close(finish); err != nil {
						t.Error(err)
					}
				}
				if err := runtime.Close(finish); err != nil {
					t.Error(err)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			reader := session.Reader()
			view, err := reader.Capture()
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := settings.As[setting.Config](view)
			if err != nil {
				t.Fatal(err)
			}
			value, err := snapshot.ValueCopy()
			if err != nil || value.Application.Workers != 2 {
				t.Fatal("project schema mismatch", err)
			}
			if settings.Configured() {
				t.Fatal("implicit process-default installation")
			}
			if err := session.Close(ctx); err != nil || !session.ShutdownComplete() {
				t.Fatal("source cleanup failed", err)
			}
			status, _ := localEvidence.Inspect()
			for range status.Queued {
				record, err := localEvidence.NextReleased(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := record.Ack(); err != nil {
					t.Fatal(err)
				}
			}
			remoteStatus, _ := remoteEvidence.Inspect()
			for range remoteStatus.Queued {
				record, err := remoteEvidence.NextReleased(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := record.Ack(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := reader.Capture(); err != nil {
				t.Fatal("closed producer destroyed accepted data", err)
			}
		})
	}
}
