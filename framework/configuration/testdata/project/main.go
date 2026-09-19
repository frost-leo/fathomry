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

// This independently compiled business project declares settings and sources;
// it does not construct SDKs, resource factories or evidence receivers.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/frost-leo/fathomry/adapters/configuration/local"
	"github.com/frost-leo/fathomry/failure"
	"github.com/frost-leo/fathomry/framework/configuration"
)

type settings struct {
	Label          string            `json:"label"`
	TimeoutSeconds int64             `json:"timeout_seconds"`
	Headers        map[string]string `json:"headers"`
	Items          []string          `json:"items"`
}

func projectSettings() configuration.Schema[settings] {
	return configuration.Schema[settings]{
		SchemaVersion: 1,
		Defaults:      settings{Label: "default", TimeoutSeconds: 30, Headers: map[string]string{"Default": "kept"}, Items: []string{"default"}},
		Validate: func(value settings) error {
			if value.TimeoutSeconds <= 0 {
				return failure.New(failure.Code("example.settings.invalid_timeout"), nil)
			}
			return nil
		},
	}
}

func loadProject(ctx context.Context, root string) (configuration.Configuration[settings], error) {
	provider, err := local.New(local.Options{
		Root: root,
		Files: []local.File{
			{Name: "base", Path: "settings.yaml", Layer: configuration.Base},
			{Name: "environment", Path: "environment.yaml", Layer: configuration.Environment, Optional: true},
			{Name: "local", Path: "settings.local.yaml", Layer: configuration.Local, Optional: true},
		},
	})
	if err != nil {
		return configuration.Configuration[settings]{}, err
	}
	return configuration.Load(ctx, projectSettings(), configuration.Request{
		Provider:  provider,
		Variables: []configuration.Variable{{Name: "FATHOMRY_EXAMPLE_LABEL", Field: "/label"}},
	})
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "supply one absolute configuration directory")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	loaded, err := loadProject(ctx, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	value, err := loaded.Value()
	if err != nil || value.TimeoutSeconds <= 0 {
		fmt.Fprintln(os.Stderr, "configuration unavailable")
		os.Exit(1)
	}
	info := loaded.Description()
	fmt.Printf("configuration loaded: provider=%s schema=%d\n", info.Provider, info.SchemaVersion)
}
