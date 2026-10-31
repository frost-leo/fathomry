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
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
)

// BootstrapInput selects a required absolute file, explicitly captured variables and
// an optional highest-priority environment. Missing map keys are absent; empty
// values are explicit. Inputs are borrowed without concurrent mutation.
type BootstrapInput struct {
	Path        string
	Variables   map[string]string
	Environment *string
}

// Bootstrap selects the application's main configuration and its load/close budgets.
// LoadBootstrap returns detached data. LoadConfiguration validates and copies it
// again before source access, so later caller edits do not retarget a session.
// Explicit access/JSON may reveal credentials; ordinary formatting is restricted.
// It is data, not a resource owner or an application host.
// InitialTimeout and CandidateTimeout must be 1ms-5m; ShutdownTimeout 1ms-1m.
// Zero is invalid when passed to LoadConfiguration, not an implicit default.
type Bootstrap struct {
	Configuration    SourcePlan
	InitialTimeout   time.Duration
	CandidateTimeout time.Duration
	ShutdownTimeout  time.Duration
}

var dotenvKeys = []string{
	"FATHOMRY_BOOTSTRAP_VERSION", "FATHOMRY_ENV", "FATHOMRY_CONFIG_MODE",
	"FATHOMRY_CONFIG_PROVIDER", "FATHOMRY_CONFIG_WATCH", "FATHOMRY_CONFIG_DIR",
	"FATHOMRY_CONFIG_DOCUMENTS_JSON", "FATHOMRY_NACOS_SETTINGS_JSON",
	"FATHOMRY_STARTUP_TIMEOUT", "FATHOMRY_CONFIG_CANDIDATE_TIMEOUT", "FATHOMRY_SHUTDOWN_TIMEOUT",
}

// BootstrapKeys returns an owned copy of the allowed dotenv/process names.
// It never reads or mutates process environment.
func BootstrapKeys() []string { return slices.Clone(dotenvKeys) }

// LoadBootstrap reads the selected literal dotenv file and resolves the application's
// bootstrap selection. It does not load main configuration or start services.
// Every supplied layer is admitted before precedence; file acquisition uses the
// borrowed Viper dependencies and the caller's context budget.
// Environment overrides Variables, which override the file. Relative directories
// resolve against the selected dotenv parent, never HOME or executable location.
// Source evidence stays in the caller's Inbox until explicitly handled/Acked.
func LoadBootstrap(
	ctx context.Context,
	input BootstrapInput,
	dependencies viper.Dependencies,
) (Bootstrap, error) {
	if err := admitDotenvInput(ctx, input); err != nil {
		return Bootstrap{}, err
	}
	input = copyDotenv(input)
	if err := localBudget(dependencies, false); err != nil {
		return Bootstrap{}, err
	}
	client, err := viper.New(dependencies)
	if err != nil {
		return Bootstrap{}, err
	}
	source, err := client.Source(viper.WatchSettings{Paths: []string{input.Path}})
	if err != nil {
		return Bootstrap{}, err
	}
	batch, index, problem := source.Capture(ctx)
	if problem != nil {
		return Bootstrap{}, at(ErrSource, "dotenv.acquire", index, problem)
	}
	if index != -1 || !batch.Valid() || batch.Len() != 1 {
		return Bootstrap{}, at(ErrObservation, "dotenv.acquire", -1)
	}
	documents, err := batch.DocumentsCopy()
	if err != nil {
		return Bootstrap{}, err
	}
	if documents[0].Missing {
		return Bootstrap{}, at(ErrMissing, "dotenv.acquire", 0)
	}
	values, err := configsource.ParseDotenv(documents[0].Content)
	if err != nil {
		return Bootstrap{}, at(ErrDeclaration, "dotenv.decode", 0, err)
	}
	if err := admitDotenvValues(ctx, values); err != nil {
		return Bootstrap{}, err
	}
	for key, value := range input.Variables {
		values[key] = value
	}
	if input.Environment != nil {
		values["FATHOMRY_ENV"] = *input.Environment
	}
	return resolveBootstrap(ctx, filepath.Dir(input.Path), values)
}

func copyDotenv(value BootstrapInput) BootstrapInput {
	if value.Environment != nil {
		environment := *value.Environment
		value.Environment = &environment
	}
	variables := make(map[string]string, len(value.Variables))
	for key, text := range value.Variables {
		variables[key] = text
	}
	value.Variables = variables
	return value
}

func admitDotenvInput(ctx context.Context, input BootstrapInput) error {
	if ctx == nil {
		return at(ErrDeclaration, "dotenv", -1)
	}
	if err := ctx.Err(); err != nil {
		return at(ErrWait, "dotenv", -1, err, context.Cause(ctx))
	}
	if err := validatePaths([]string{input.Path}); err != nil {
		return err
	}
	if input.Environment != nil && !environment(*input.Environment) {
		return at(ErrDeclaration, "environment", -1)
	}
	return admitDotenvValues(ctx, input.Variables)
}

func admitDotenvValues(ctx context.Context, values map[string]string) error {
	if len(values) > configsource.MaxVariables {
		return at(ErrLimit, "dotenv.variables", -1)
	}
	size := 0
	for key, value := range values {
		if err := ctx.Err(); err != nil {
			return at(ErrWait, "dotenv.variables", -1, err, context.Cause(ctx))
		}
		if !slices.Contains(dotenvKeys, key) || !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
			return at(ErrDeclaration, "dotenv.variable", -1)
		}
		if len(key) > configsource.MaxDotenvBytes-size {
			return at(ErrLimit, "dotenv.variables", -1)
		}
		size += len(key)
		if len(value) > configsource.MaxDotenvBytes-size {
			return at(ErrLimit, "dotenv.variables", -1)
		}
		size += len(value)
		switch key {
		case "FATHOMRY_BOOTSTRAP_VERSION":
			if value != "1" {
				return at(ErrDeclaration, "dotenv.version", -1)
			}
		case "FATHOMRY_ENV":
			if !environment(value) {
				return at(ErrDeclaration, "environment", -1)
			}
		case "FATHOMRY_CONFIG_MODE":
			if value != "local" && value != "remote" {
				return at(ErrDeclaration, "dotenv.mode", -1)
			}
		case "FATHOMRY_CONFIG_PROVIDER":
			if value != "viper" && value != "nacos" {
				return at(ErrDeclaration, "dotenv.provider", -1)
			}
		case "FATHOMRY_CONFIG_WATCH":
			if value != "true" && value != "false" {
				return at(ErrDeclaration, "dotenv.watch", -1)
			}
		case "FATHOMRY_CONFIG_DIR":
			if value == "" || len(value) > 4096 {
				return at(ErrDeclaration, "dotenv.directory", -1)
			}
		case "FATHOMRY_CONFIG_DOCUMENTS_JSON":
			if _, err := dotenvDocuments(ctx, value); err != nil {
				return err
			}
		case "FATHOMRY_NACOS_SETTINGS_JSON":
			if _, err := dotenvRemote(ctx, value); err != nil {
				return err
			}
		default:
			maximum := 5 * time.Minute
			if key == "FATHOMRY_SHUTDOWN_TIMEOUT" {
				maximum = time.Minute
			}
			if _, err := dotenvDuration(value, maximum); err != nil {
				return err
			}
		}
	}
	return nil
}

func dotenvDuration(value string, maximum time.Duration) (time.Duration, error) {
	duration, err := time.ParseDuration(value)
	if err != nil || duration < time.Millisecond || duration > maximum {
		return 0, at(ErrDeclaration, "dotenv.duration", -1, err)
	}
	return duration, nil
}
func dotenvRemote(ctx context.Context, value string) (nacos.Settings, error) {
	prepared, err := configsource.Prepare(ctx, configsource.Schema[nacos.Settings]{Version: 1},
		[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(value)}})
	if err != nil {
		return nacos.Settings{}, at(ErrDeclaration, "dotenv.remote", -1, err)
	}
	return prepared.ValueCopy()
}
func dotenvDocuments(ctx context.Context, value string) ([]Document, error) {
	type documentList struct {
		Documents []Document `json:"documents"`
	}
	prepared, err := configsource.Prepare(ctx, configsource.Schema[documentList]{Version: 1},
		[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte("{\"documents\":" + value + "}")}})
	if err != nil {
		return nil, at(ErrDeclaration, "dotenv.documents", -1, err)
	}
	decoded, err := prepared.ValueCopy()
	return decoded.Documents, err
}
func resolveBootstrap(ctx context.Context, directory string, values map[string]string) (Bootstrap, error) {
	result := Bootstrap{InitialTimeout: 30 * time.Second, CandidateTimeout: 5 * time.Second, ShutdownTimeout: 10 * time.Second,
		Configuration: SourcePlan{Version: 1, Environment: values["FATHOMRY_ENV"], Mode: values["FATHOMRY_CONFIG_MODE"],
			Provider: values["FATHOMRY_CONFIG_PROVIDER"], Watch: values["FATHOMRY_CONFIG_WATCH"] == "true"}}
	for _, field := range []struct {
		key     string
		target  *time.Duration
		maximum time.Duration
	}{
		{"FATHOMRY_STARTUP_TIMEOUT", &result.InitialTimeout, 5 * time.Minute},
		{"FATHOMRY_CONFIG_CANDIDATE_TIMEOUT", &result.CandidateTimeout, 5 * time.Minute},
		{"FATHOMRY_SHUTDOWN_TIMEOUT", &result.ShutdownTimeout, time.Minute},
	} {
		if value, present := values[field.key]; present {
			duration, err := dotenvDuration(value, field.maximum)
			if err != nil {
				return Bootstrap{}, err
			}
			*field.target = duration
		}
	}
	switch result.Configuration.Mode {
	case "local":
		if _, present := values["FATHOMRY_CONFIG_DOCUMENTS_JSON"]; present {
			return Bootstrap{}, at(ErrDeclaration, "dotenv.branch", -1)
		}
		if _, present := values["FATHOMRY_NACOS_SETTINGS_JSON"]; present {
			return Bootstrap{}, at(ErrDeclaration, "dotenv.branch", -1)
		}
		path, present := values["FATHOMRY_CONFIG_DIR"]
		if !present {
			path = "config"
		}
		if !filepath.IsAbs(path) {
			path = filepath.Join(directory, path)
		}
		result.Configuration.Local = &viper.WatchSettings{Paths: []string{
			filepath.Join(path, "base.yaml"), filepath.Join(path, "environment", result.Configuration.Environment+".yaml")}}
		result.Configuration.Documents = []Document{{Kind: "base", Encoding: YAML}, {Kind: "environment", Encoding: YAML}}
	case "remote":
		if _, present := values["FATHOMRY_CONFIG_DIR"]; present {
			return Bootstrap{}, at(ErrDeclaration, "dotenv.branch", -1)
		}
		documents, err := dotenvDocuments(ctx, values["FATHOMRY_CONFIG_DOCUMENTS_JSON"])
		if err != nil {
			return Bootstrap{}, err
		}
		remote, err := dotenvRemote(ctx, values["FATHOMRY_NACOS_SETTINGS_JSON"])
		if err != nil {
			return Bootstrap{}, err
		}
		result.Configuration.Documents = documents
		result.Configuration.Remote = &Remote{Settings: remote}
	}
	selected, err := preparePlan(ctx, result.Configuration)
	if err != nil {
		return Bootstrap{}, err
	}
	result.Configuration = selected
	return result, nil
}
