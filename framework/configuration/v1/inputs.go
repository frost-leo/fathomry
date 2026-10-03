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
	"flag"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"unicode/utf8"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

// InputBinding grants one startup input explicit names and precedence.
// Name is the lookup key. Flag and Environment may be absent. Dotenv authorizes
// the Environment name in an explicitly selected file. HasDefault distinguishes
// absent from an explicitly empty default. No names are inferred.
type InputBinding struct {
	Name        string
	Flag        string
	Environment string
	Default     string
	HasDefault  bool
	Dotenv      bool
}

// InputOptions supplies caller-owned arguments and lookup, not process authority.
// Up to 32 bindings/arguments are admitted. Raw argument bytes and selected text
// each have an independent 256 KiB limit; each selected value permits 64 KiB.
// Lookup is synchronous, bounded and called at most once per declared name.
// DotenvFlag/Environment explicitly authorize selection of one literal file.
// Relative paths resolve against the current working directory, never ancestors.
type InputOptions struct {
	Arguments         []string
	Lookup            func(string) (string, bool)
	Bindings          []InputBinding
	DotenvFlag        string
	DotenvEnvironment string
}

// Inputs retains selected startup values separately from released file-read facts.
// Values may contain credentials. Formatting is guarded; Lookup deliberately
// exposes a string. Copies share immutable data and are safe for concurrent reads.
type Inputs struct {
	private
	values  map[string]string
	records []Record
}

func (inputs Inputs) Lookup(name string) (string, bool) {
	value, present := inputs.values[name]
	return value, present
}

// Records transfers detached in-process facts, not a durable audit guarantee.
func (inputs Inputs) Records() []Record { return slices.Clone(inputs.records) }

// ReadInputs applies arguments > supplied lookup > selected dotenv > defaults.
// It never reads os.Environ, mutates it, chooses a provider, or loads an example
// implicitly. Invalid/unknown selected-file assignments reject, even if shadowed.
// No application configuration is loaded or published by this function.
func ReadInputs(ctx context.Context, options InputOptions) (result Inputs, err error) {
	if ctx == nil || options.Lookup == nil || len(options.Bindings) > 32 || len(options.Arguments) > 32 {
		return result, fail(ErrDeclaration, "inputs")
	}
	if err := ctx.Err(); err != nil {
		return result, fail(ErrWait, "inputs", err, context.Cause(ctx))
	}
	names, flags, environments := map[string]bool{}, map[string]bool{}, map[string]bool{}
	if options.DotenvFlag != "" {
		if !inputName(options.DotenvFlag, false) {
			return result, fail(ErrDeclaration, "inputs")
		}
		flags[options.DotenvFlag] = true
	}
	if options.DotenvEnvironment != "" {
		if !inputName(options.DotenvEnvironment, true) {
			return result, fail(ErrDeclaration, "inputs")
		}
		environments[options.DotenvEnvironment] = true
	}
	for _, binding := range options.Bindings {
		if !inputName(binding.Name, false) || names[binding.Name] ||
			binding.Flag != "" && (!inputName(binding.Flag, false) || flags[binding.Flag]) ||
			binding.Environment != "" && (!inputName(binding.Environment, true) || environments[binding.Environment]) ||
			binding.Dotenv && binding.Environment == "" ||
			binding.HasDefault && !inputText(binding.Default) {
			return result, fail(ErrDeclaration, "inputs")
		}
		names[binding.Name] = true
		if binding.Flag != "" {
			flags[binding.Flag] = true
		}
		if binding.Environment != "" {
			environments[binding.Environment] = true
		}
	}
	supplied := map[string]string{}
	var flagError error
	parser := flag.NewFlagSet("configuration", flag.ContinueOnError)
	parser.SetOutput(io.Discard)
	var dotenv string
	dotenvPresent := false
	if options.DotenvFlag != "" {
		parser.Func(options.DotenvFlag, "", func(value string) error {
			if dotenvPresent {
				return fail(ErrDeclaration, "inputs")
			}
			if !inputText(value) {
				flagError = fail(ErrLimit, "inputs")
				return flagError
			}
			dotenv, dotenvPresent = value, true
			return nil
		})
	}
	for _, binding := range options.Bindings {
		if binding.Flag != "" {
			parser.Func(binding.Flag, "", func(value string) error {
				if _, present := supplied[binding.Name]; present {
					return fail(ErrDeclaration, "inputs")
				}
				if !inputText(value) {
					flagError = fail(ErrLimit, "inputs")
					return flagError
				}
				supplied[binding.Name] = value
				return nil
			})
		}
	}
	total := 0
	for _, arg := range options.Arguments {
		if len(arg) > (256<<10)-total || !utf8.ValidString(arg) || strings.ContainsRune(arg, 0) {
			return result, fail(ErrLimit, "inputs")
		}
		total += len(arg)
	}
	if err := parser.Parse(options.Arguments); err != nil || parser.NArg() != 0 {
		// flag formats callback errors as text; retain the original limit identity.
		if flagError != nil {
			return result, flagError
		}
		return result, fail(ErrDeclaration, "inputs", err)
	}
	if options.DotenvEnvironment != "" {
		value, present := options.Lookup(options.DotenvEnvironment)
		if !dotenvPresent && present {
			dotenv = value
		}
	}
	literal := map[string]string{}
	if dotenv != "" {
		if len(dotenv) > 4096 || !inputText(dotenv) {
			return result, fail(ErrDeclaration, "dotenv")
		}
		raw, records, err := readInputDocument(ctx, dotenv)
		result.records = records
		if err != nil {
			return result, err
		}
		literal, err = configsource.ParseDotenv(raw)
		if err != nil {
			return result, err
		}
	}
	allowed := map[string]bool{}
	values := map[string]string{}
	total = 0
	for _, binding := range options.Bindings {
		value, present := binding.Default, binding.HasDefault
		if binding.Dotenv {
			allowed[binding.Environment] = true
			if text, exists := literal[binding.Environment]; exists {
				value, present = text, true
			}
		}
		if binding.Environment != "" {
			if text, exists := options.Lookup(binding.Environment); exists {
				value, present = text, true
			}
		}
		if text, exists := supplied[binding.Name]; exists {
			value, present = text, true
		}
		if present {
			total += len(value)
			if !inputText(value) || total > 256<<10 {
				return result, fail(ErrLimit, "inputs")
			}
			values[binding.Name] = value
		}
	}
	for name := range literal {
		if !allowed[name] {
			return result, fail(ErrDeclaration, "dotenv_names")
		}
	}
	if err := ctx.Err(); err != nil {
		return result, fail(ErrWait, "inputs", err, context.Cause(ctx))
	}
	result.values = values
	return result, nil
}

func inputText(value string) bool {
	return len(value) <= 64<<10 && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}
func inputName(value string, environment bool) bool {
	if len(value) == 0 || len(value) > 64 {
		return false
	}
	for index, char := range value {
		if char >= 'a' && char <= 'z' || char == '_' || environment && char >= 'A' && char <= 'Z' ||
			index > 0 && (char >= '0' && char <= '9' || !environment && char == '-') {
			continue
		}
		return false
	}
	return true
}
func readInputDocument(ctx context.Context, path string) (raw []byte, records []Record, err error) {
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, nil, fail(ErrDeclaration, "dotenv", err)
	}
	scenario, err := newScenario(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		var cleanup error
		records, cleanup = scenario.finish(context.WithoutCancel(ctx))
		if cleanup != nil {
			err = fail(ErrCleanup, "inputs", err, cleanup)
		}
	}()
	inbox, err := adapters.NewInbox[viper.Evidence](adapters.EvidenceOptions{Capacity: 1, MaxBytes: 64 << 10})
	if err != nil {
		return nil, nil, err
	}
	scenario.binding.records = func(ctx context.Context) ([]Record, error) {
		return takeRecords(ctx, inbox, "viper", func(value viper.Evidence) recordFacts {
			return recordFacts{source: SourceEvidence{Documents: value.Documents, Missing: value.Missing, FailedIndex: -1}, sourcePresent: true}
		})
	}
	client, err := viper.New(viper.Dependencies{Runtime: scenario.runtime, Evidence: inbox})
	if err != nil {
		return nil, nil, err
	}
	var missing bool
	raw, missing, err = client.RawFile(ctx, path, configsource.MaxDotenvBytes)
	if err == nil && missing {
		err = fail(ErrMissing, "dotenv")
	}
	return raw, nil, err
}
