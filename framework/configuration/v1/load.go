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

package configuration

import (
	"context"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
)

// LoadBootstrap reads explicit local inputs synchronously using borrowed Viper
// dependencies, including when the resulting application plan is remote.
// Cancellation is cooperative, not forced interruption of blocked filesystem I/O.
// Required acquisition evidence remains in the supplied Inbox until caller Ack.
func LoadBootstrap(ctx context.Context, input BootstrapInput, dependencies viper.Dependencies) (Bootstrap, error) {
	if ctx == nil || !environment(input.Environment) || len(input.Files) < 1 || len(input.Files) > 3 {
		return Bootstrap{}, at(ErrDeclaration, "bootstrap", -1)
	}
	paths := make([]string, len(input.Files))
	documents := make([]Document, len(input.Files))
	for index, file := range input.Files {
		paths[index], documents[index] = file.Path, file.Document
	}
	if err := validateDocuments(documents); err != nil {
		return Bootstrap{}, err
	}
	if err := validatePaths(paths); err != nil {
		return Bootstrap{}, err
	}
	variables, err := configsource.BindVariables[Bootstrap](input.Variables)
	if err != nil {
		return Bootstrap{}, err
	}
	if err := localBudget(dependencies, false); err != nil {
		return Bootstrap{}, err
	}
	if err := ctx.Err(); err != nil {
		return Bootstrap{}, at(ErrWait, "bootstrap", -1, err, context.Cause(ctx))
	}
	client, err := viper.New(dependencies)
	if err != nil {
		return Bootstrap{}, err
	}
	source, err := client.Source(viper.WatchSettings{Paths: paths})
	if err != nil {
		return Bootstrap{}, err
	}
	batch, index, problem := source.Capture(ctx)
	layers, err := originalLayers(batch, index, problem, documents, "bootstrap.acquire")
	if err != nil {
		return Bootstrap{}, err
	}
	if len(input.Variables) != 0 {
		layers = append(layers, variables)
	}
	schema := configsource.Schema[Bootstrap]{
		Version: 1, Defaults: Bootstrap{Environment: input.Environment},
		Validate: func(_ context.Context, value Bootstrap) error {
			if value.Environment != input.Environment {
				return at(ErrDeclaration, "environment", -1)
			}
			return validateBootstrap(value)
		},
	}
	prepared, err := configsource.Prepare(ctx, schema, layers)
	if err != nil {
		return Bootstrap{}, err
	}
	return prepared.ValueCopy()
}

func freezeBootstrap(ctx context.Context, value Bootstrap) (Bootstrap, error) {
	prepared, err := configsource.Prepare(ctx, configsource.Schema[Bootstrap]{
		Version: 1, Defaults: value, Validate: func(_ context.Context, candidate Bootstrap) error { return validateBootstrap(candidate) },
	}, nil)
	if err != nil {
		return Bootstrap{}, err
	}
	return prepared.ValueCopy()
}

func validateBootstrap(value Bootstrap) error {
	if value.Version != 1 || !environment(value.Environment) {
		return at(ErrDeclaration, "bootstrap", -1)
	}
	if err := validateDocuments(value.Documents); err != nil {
		return err
	}
	switch {
	case value.Mode == "local" && value.Provider == "viper":
		if value.Local == nil || value.Remote != nil || len(value.Local.Paths) != len(value.Documents) {
			return at(ErrDeclaration, "local", -1)
		}
		if err := validatePaths(value.Local.Paths); err != nil {
			return err
		}
		interval, capacity := value.Local.Interval, value.Local.QueueCapacity
		if interval != 0 && (interval < 10*time.Millisecond || interval > 5*time.Minute) || capacity < 0 || capacity > 64 {
			return at(ErrDeclaration, "local", -1)
		}
	case value.Mode == "remote" && value.Provider == "nacos":
		if value.Remote == nil || value.Local != nil || len(value.Remote.Settings.Keys) != len(value.Documents) ||
			value.Remote.Settings.Writable || value.Remote.Settings.DynamicKeys ||
			value.Remote.Observation.QueueCapacity < 0 || value.Remote.Observation.QueueCapacity > 16 {
			return at(ErrDeclaration, "remote", -1)
		}
		if err := nacos.Validate(value.Remote.Settings); err != nil {
			return err
		}
	default:
		return at(ErrDeclaration, "provider", -1)
	}
	return nil
}

func environment(value string) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for _, letter := range value {
		if !(letter >= 'a' && letter <= 'z' || letter >= 'A' && letter <= 'Z' ||
			letter >= '0' && letter <= '9' || letter == '.' || letter == '_' || letter == '-') {
			return false
		}
	}
	return true
}
func validatePaths(paths []string) error {
	seen := make(map[string]bool, len(paths))
	total := 0
	for index, path := range paths {
		if len(path) > 4096 || !filepath.IsAbs(path) || !utf8.ValidString(path) || strings.ContainsRune(path, 0) || seen[path] {
			return at(ErrDeclaration, "path", index)
		}
		total += len(path)
		if total > viper.MaxBootstrapBytes {
			return at(ErrLimit, "paths", index)
		}
		seen[path] = true
	}
	return nil
}
func layerKind(kind string) configsource.LayerKind {
	switch kind {
	case "base":
		return configsource.Base
	case "environment":
		return configsource.Environment
	case "local":
		return configsource.Local
	}
	return 0
}
func validateDocuments(documents []Document) error {
	if len(documents) < 1 || len(documents) > 3 {
		return at(ErrDeclaration, "documents", -1)
	}
	seen := make(map[string]bool, len(documents))
	for index, document := range documents {
		if layerKind(document.Kind) == 0 || seen[document.Kind] ||
			document.Encoding != configsource.YAML && document.Encoding != configsource.JSON && document.Encoding != configsource.TOML {
			return at(ErrDeclaration, "document", index)
		}
		seen[document.Kind] = true
	}
	return nil
}

func originalLayers(batch configsource.Batch, index int, problem error, documents []Document, operation string) ([]configsource.Layer, error) {
	if index < -1 || index >= len(documents) || problem == nil && index != -1 {
		return nil, at(ErrObservation, operation, -1, problem)
	}
	if problem != nil {
		return nil, at(ErrSource, operation, index, problem)
	}
	if !batch.Valid() || batch.Len() != len(documents) {
		return nil, at(ErrObservation, operation, -1)
	}
	raw, err := batch.DocumentsCopy()
	if err != nil {
		return nil, at(ErrObservation, operation, -1, err)
	}
	layers := make([]configsource.Layer, 0, len(raw))
	for index, value := range raw {
		if value.Missing {
			if !documents[index].Optional {
				return nil, at(ErrMissing, operation, index)
			}
			continue
		}
		layers = append(layers, configsource.Layer{Kind: layerKind(documents[index].Kind), Encoding: documents[index].Encoding, Content: value.Content})
	}
	return layers, nil
}
