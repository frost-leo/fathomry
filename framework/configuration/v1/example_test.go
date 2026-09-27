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

package configuration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	local "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	adapters "github.com/frost-leo/fathomry/adapters/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
)

func ExampleLoad() {
	type project struct {
		BatchSize int `json:"batch_size"`
	}
	schema := configuration.Schema[project]{FormatVersion: 1, Defaults: configuration.DefaultSettings(project{BatchSize: 100})}
	snapshot, err := configuration.Load(context.Background(), schema, configuration.Plan{})
	if err != nil {
		panic(err)
	}
	settings, err := snapshot.ValueCopy()
	if err != nil {
		panic(err)
	}
	presentation, err := snapshot.Presentation()
	if err != nil {
		panic(err)
	}
	locale, err := presentation.Locale()
	if err != nil {
		panic(err)
	}
	fmt.Println(settings.Project.BatchSize, locale)
	// Output: 100 en
}

func ExampleWatch() {
	type project struct {
		BatchSize int `json:"batch_size"`
	}
	directory, err := os.MkdirTemp("", "fathomry-configuration-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "settings.yaml")
	if err := os.WriteFile(path, []byte("format: 1\nproject: {batch_size: 25}"), 0600); err != nil {
		panic(err)
	}
	selected, err := local.Select(local.Settings{Name: "files", Documents: []local.File{{Name: "base", Path: path, Encoding: "yaml"}}, ReconcileInterval: time.Second})
	if err != nil {
		panic(err)
	}
	schema := configuration.Schema[project]{FormatVersion: 1, Defaults: configuration.DefaultSettings(project{BatchSize: 100})}
	plan := configuration.Plan{Modules: []adapters.Module{local.Module()}, Inputs: []configuration.Input{{Source: selected, Documents: []configuration.LayerDocument{{Document: "base", Layer: configuration.Base}}}}}
	live, err := configuration.Watch(context.Background(), schema, plan)
	if err != nil {
		panic(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := live.Close(cleanup); err != nil {
			panic(err)
		}
	}()
	startup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	state, err := live.Current()
	for err == nil && state.Status != configuration.Ready {
		if state.Status == configuration.Closing || state.Status == configuration.Closed {
			panic("configuration stopped before readiness")
		}
		state, err = live.Next(startup, state.Cursor)
	}
	if err != nil {
		panic(err)
	}
	settings, err := state.Snapshot.ValueCopy()
	if err != nil {
		panic(err)
	}
	fmt.Println(settings.Project.BatchSize)
	// Output: 25
}
