/*
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

package project

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourcePlanRejectsInvalidCombinationsBeforeWriting(t *testing.T) {
	for _, mutate := range []func(*Options){
		func(o *Options) { o.Configuration = "remote"; o.Provider = "viper" },
		func(o *Options) { o.Configuration = "local"; o.Provider = "nacos" },
		func(o *Options) { o.DefaultLocale = "unsupported" },
		func(o *Options) { o.EnvironmentSources = []string{"unknown=remote/nacos"} },
		func(o *Options) { o.EnvironmentSources = []string{"production=local/nacos"} },
		func(o *Options) { o.EnvironmentSources = []string{"production=remote/nacos", "production=local/viper"} },
		func(o *Options) {
			o.EnvironmentSources = []string{"development=local/viper", "test=local/viper", "production=remote/nacos", "production=remote/nacos"}
		},
		func(o *Options) { o.EnvironmentSources = []string{"production=remote/nacos/extra"} },
	} {
		options := optionsFor(t)
		mutate(&options)
		result, err := Create(context.Background(), options)
		if !errors.Is(err, InvalidInput) || result.Created {
			t.Fatal("invalid selection accepted", err)
		}
		if _, err := os.Stat(options.Directory); !errors.Is(err, fs.ErrNotExist) {
			t.Fatal("invalid selection wrote files")
		}
	}
}

func TestSourcePlansGenerateOnlySelectedCapabilities(t *testing.T) {
	for _, mode := range []string{"local", "remote", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			options := optionsFor(t)
			options.Provider = ""
			options.Configuration = mode
			if mode == "mixed" {
				options.Configuration = "local"
				options.EnvironmentSources = []string{"production=remote/nacos"}
			}
			options.DefaultLocale = "zh-Hans"
			result, err := Create(context.Background(), options)
			if err != nil || !result.Complete {
				t.Fatal("generation failed", err)
			}
			for file, want := range map[string]bool{
				"internal/configuration/sources_local.go": mode != "remote",
				"internal/configuration/sources_nacos.go": mode != "local",
			} {
				_, err := os.Stat(filepath.Join(options.Directory, file))
				if want && err != nil || !want && !errors.Is(err, fs.ErrNotExist) {
					t.Fatal("wrong compiled provider set", file, err)
				}
			}
			for _, file := range []string{".env.example", ".env.development.example", ".env.test.example", ".env.production.example", "internal/resource/configuration.go"} {
				data, err := os.ReadFile(filepath.Join(options.Directory, file))
				if err != nil || !strings.Contains(string(data), "zh-Hans") {
					t.Fatal("missing real configuration content", file)
				}
			}
			if _, err := os.Stat(filepath.Join(options.Directory, ".env")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatal("active private dotenv generated")
			}
			source, err := os.ReadFile(filepath.Join(options.Directory, "internal/resource/configuration.go"))
			if err != nil || !strings.Contains(string(source), "i18n.Settings") || strings.Contains(string(source), "ProjectValues") {
				t.Fatal("capability settings were duplicated or replaced with demonstration fields")
			}
		})
	}
}
