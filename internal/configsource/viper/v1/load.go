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

// Package viper integrates the Viper v1 SDK with bounded, isolated reads and live
// queries. It does not assign application layers or prepare business settings.
// Callers select static inputs explicitly; no discovery, merge, watcher, writeback,
// SDK handle, managed resource, or process-global configuration is exposed.
package viper

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"unicode/utf8"

	sdk "github.com/spf13/viper"
	"go.yaml.in/yaml/v3"
)

const (
	MaxSources        = 16
	MaxDocumentBytes  = 1 << 20
	MaxTotalBytes     = 4 << 20
	MaxBootstrapBytes = 64 << 10
	MaxEntries        = 64
	MaxKeyBytes       = 256
	MaxDepth          = 64
	MaxNodes          = 32768
)

// LoadInput selects exactly one literal absolute File path or caller-owned Reader.
// Options selects option contract revision 1, independently of SDK version. Files
// must be stable regular files; symlinks follow ordinary OS semantics. Readers
// are borrowed until Load returns, never closed, and must obey io.Reader's contract.
// All input storage is borrowed during Load only; do not mutate it concurrently.
type LoadInput struct {
	private
	Options OptionsV1
	File    string
	Reader  io.Reader
}

// Load acquires all inputs in order, with a fresh Viper for each, or returns nil.
// It never merges documents or returns a partially usable new batch. Each document
// retains original bytes separately from native normalized state. Required/optional
// source selection belongs to the caller; no error is interpreted as optional here.
//
// Byte limits apply before/during acquisition; structural limits apply before SDK
// decoding. Files are closed before return, including failures. No goroutine or
// persistent resource is created. Context is checked between synchronous phases
// and reads; it cannot interrupt an arbitrary blocked Read, Open, Stat, Close or
// parser. Sources/environment must remain static when common preparation is needed;
// success does not establish a common-time snapshot.
func Load(ctx context.Context, inputs []LoadInput) ([]*Document, error) {
	if ctx == nil {
		return nil, fail(ErrInput, "load")
	}
	if err := validateInputs(inputs); err != nil {
		return nil, err
	}
	documents := make([]*Document, 0, len(inputs))
	remaining := MaxTotalBytes
	for _, input := range inputs {
		if err := ctx.Err(); err != nil {
			return nil, fail(ErrRead, "load", err, context.Cause(ctx))
		}
		limit := min(MaxDocumentBytes, remaining)
		var raw []byte
		var err error
		if input.File != "" {
			raw, err = readFile(ctx, strings.Clone(input.File), limit)
		} else {
			raw, err = readBounded(ctx, input.Reader, limit)
		}
		if err != nil {
			return nil, err
		}
		remaining -= len(raw)
		if err := checkStructure(raw, input.Options.Encoding); err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, fail(ErrRead, "load", err, context.Cause(ctx))
		}
		native := sdk.New()
		encoding := strings.Clone(input.Options.Encoding)
		native.SetConfigType(encoding)
		native.AllowEmptyEnv(input.Options.AllowEmptyEnv)
		for _, entry := range input.Options.Defaults {
			value := entry.Value
			if text, ok := value.(string); ok {
				value = strings.Clone(text)
			}
			native.SetDefault(strings.Clone(entry.Key), value)
		}
		for _, binding := range input.Options.Environment {
			if err := native.BindEnv(strings.Clone(binding.Key), strings.Clone(binding.Name)); err != nil {
				return nil, fail(ErrInput, "bind", err)
			}
		}
		if err := native.ReadConfig(bytes.NewReader(raw)); err != nil {
			return nil, fail(ErrDecode, "decode", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fail(ErrRead, "load", err, context.Cause(ctx))
		}
		documents = append(documents, &Document{raw: raw, encoding: encoding, native: native})
	}
	return documents, nil
}

func validateInputs(inputs []LoadInput) error {
	if len(inputs) == 0 || len(inputs) > MaxSources {
		return fail(ErrInput, "load")
	}
	budget := MaxBootstrapBytes
	for _, input := range inputs {
		if input.Options.Encoding != "yaml" && input.Options.Encoding != "json" ||
			(input.File == "") == nilReader(input.Reader) ||
			input.File != "" && (!validPath(input.File) || strings.ContainsRune(input.File, 0)) ||
			len(input.Options.Defaults) > MaxEntries || len(input.Options.Environment) > MaxEntries {
			return fail(ErrInput, "setup")
		}
		budget -= len(input.File)
		for _, entry := range input.Options.Defaults {
			size, ok := scalarSize(entry.Value)
			if !validKey(entry.Key) || !ok {
				return fail(ErrInput, "defaults")
			}
			if size > budget || len(entry.Key) > budget-size {
				return fail(ErrLimit, "setup")
			}
			budget -= len(entry.Key) + size
		}
		for _, binding := range input.Options.Environment {
			if !validKey(binding.Key) || binding.Name == "" || len(binding.Name) > MaxKeyBytes ||
				!utf8.ValidString(binding.Name) || strings.ContainsAny(binding.Name, "=\x00") {
				return fail(ErrInput, "bind")
			}
			budget -= len(binding.Key) + len(binding.Name)
		}
		if budget < 0 {
			return fail(ErrLimit, "setup")
		}
	}
	return nil
}

func nilReader(reader io.Reader) bool {
	if reader == nil {
		return true
	}
	value := reflect.ValueOf(reader)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func readFile(ctx context.Context, path string, limit int) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fail(ErrRead, "stat", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fail(ErrInput, "stat")
	}
	if info.Size() > int64(limit) {
		return nil, fail(ErrLimit, "stat")
	}
	if err := ctx.Err(); err != nil {
		return nil, fail(ErrRead, "open", err, context.Cause(ctx))
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fail(ErrRead, "open", err)
	}
	return readOwned(ctx, file, limit)
}

func readOwned(ctx context.Context, file fs.File, limit int) (raw []byte, err error) {
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			raw = nil
			err = errors.Join(err, fail(ErrClose, "close", closeErr))
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return nil, fail(ErrRead, "stat", err)
	}
	if !info.Mode().IsRegular() {
		return nil, fail(ErrInput, "stat")
	}
	if info.Size() > int64(limit) {
		return nil, fail(ErrLimit, "stat")
	}
	return readBounded(ctx, file, limit)
}

type checkedReader struct {
	ctx        context.Context
	reader     io.Reader
	emptyReads int
}

func (reader *checkedReader) Read(buffer []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	count, err := reader.reader.Read(buffer)
	if count == 0 && err == nil {
		reader.emptyReads++
		if reader.emptyReads >= 100 {
			return 0, io.ErrNoProgress
		}
	} else {
		reader.emptyReads = 0
	}
	return count, err
}

func readBounded(ctx context.Context, reader io.Reader, limit int) ([]byte, error) {
	// One extra byte distinguishes EOF at the budget from truncated input.
	raw, err := io.ReadAll(io.LimitReader(&checkedReader{ctx: ctx, reader: reader}, int64(limit)+1))
	if len(raw) > limit {
		return nil, fail(ErrLimit, "read", err, ctx.Err(), context.Cause(ctx))
	}
	if err != nil || ctx.Err() != nil {
		return nil, fail(ErrRead, "read", err, ctx.Err(), context.Cause(ctx))
	}
	return raw, nil
}

// Absolute paths are literal, not a home/environment expansion language.
func validPath(path string) bool {
	return len(path) <= 4096 && filepath.IsAbs(path)
}

// Preflight bounds decoder expansion, not business schema, field names, null,
// numeric conversion, or precedence. Only the original bytes reach the SDK.
func checkStructure(raw []byte, encoding string) error {
	if encoding == "json" {
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		depth, nodes := 0, 0
		for {
			token, err := decoder.Token()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				// Let the native codec retain its own parse-error chain.
				return nil
			}
			nodes++
			if delimiter, ok := token.(json.Delim); ok {
				switch delimiter {
				case '{', '[':
					depth++
				case '}', ']':
					depth--
				}
			}
			if depth > MaxDepth || nodes > MaxNodes {
				return fail(ErrLimit, "structure")
			}
		}
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return fail(ErrDecode, "structure", err)
	}
	nodes := 0
	var visit func(*yaml.Node, int) error
	visit = func(node *yaml.Node, depth int) error {
		nodes++
		if depth > MaxDepth || nodes > MaxNodes || len(node.Content) > MaxNodes-nodes {
			return fail(ErrLimit, "structure")
		}
		if node.Anchor != "" || node.Kind == yaml.AliasNode {
			return fail(ErrInput, "structure")
		}
		if node.Kind == yaml.MappingNode {
			// Match the native duplicate comparison without constructing its
			// potentially quadratic list of errors. Tags/case are not normalized.
			type keyIdentity struct {
				kind  yaml.Kind
				value string
			}
			seen := make(map[keyIdentity]struct{}, len(node.Content)/2)
			for index := 0; index < len(node.Content); index += 2 {
				key := node.Content[index]
				identity := keyIdentity{key.Kind, key.Value}
				if _, exists := seen[identity]; exists {
					return fail(ErrInput, "structure")
				}
				seen[identity] = struct{}{}
			}
		}
		for _, child := range node.Content {
			if err := visit(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(&root, 0); err != nil {
		return err
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fail(ErrInput, "structure", err)
	}
	return nil
}
