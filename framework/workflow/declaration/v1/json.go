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

package declaration

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
)

const (
	safeInteger int64 = 9007199254740991
	maxValues         = 1 << 20
)

type jsonBudget struct{ remaining int }

func readJSON(ctx context.Context, raw []byte, maximum, depthLimit int) (any, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, invalid("empty_document", "/")
	}
	if len(raw) > maximum {
		return nil, problem(ErrLimit, "bytes", "/")
	}
	decoder := jsontext.NewDecoder(bytes.NewReader(raw))
	budget := jsonBudget{remaining: maxValues}
	value, err := readValue(ctx, decoder, &budget, 1, depthLimit)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, problem(ErrInvalid, "trailing_value", "/", err)
	}
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	return value, nil
}

func readValue(ctx context.Context, decoder *jsontext.Decoder, budget *jsonBudget, depth, limit int) (any, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	budget.remaining--
	if budget.remaining < 0 {
		return nil, problem(ErrLimit, "values", "/")
	}
	token, err := decoder.ReadToken()
	if err != nil {
		return nil, problem(ErrInvalid, "json", "/", err)
	}
	switch token.Kind() {
	case '{', '[':
		if depth > limit {
			return nil, problem(ErrLimit, "depth", "/")
		}
		if token.Kind() == '{' {
			object := make(map[string]any)
			for decoder.PeekKind() != '}' {
				name, err := decoder.ReadToken()
				if err != nil {
					return nil, problem(ErrInvalid, "json", "/", err)
				}
				if name.Kind() != '"' {
					return nil, invalid("object_name", "/")
				}
				key := name.String()
				budget.remaining--
				value, err := readValue(ctx, decoder, budget, depth+1, limit)
				if err != nil {
					return nil, err
				}
				object[key] = value
			}
			if _, err := decoder.ReadToken(); err != nil {
				return nil, problem(ErrInvalid, "json", "/", err)
			}
			return object, nil
		}
		values := make([]any, 0)
		for decoder.PeekKind() != ']' {
			value, err := readValue(ctx, decoder, budget, depth+1, limit)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		if _, err := decoder.ReadToken(); err != nil {
			return nil, problem(ErrInvalid, "json", "/", err)
		}
		return values, nil
	case '"':
		return token.String(), nil
	case '0':
		text := token.String()
		if text == "-0" || strings.ContainsAny(text, ".eE") {
			return nil, invalid("number", "/")
		}
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil || value < -safeInteger || value > safeInteger {
			return nil, invalid("number", "/")
		}
		return value, nil
	case 't', 'f':
		return token.Bool(), nil
	case 'n':
		return nil, nil
	default:
		return nil, invalid("json", "/")
	}
}

// Defaults can expand a compiled bundle beyond the source's value budget. Check
// the final encoding without allocating another decoded document tree.
func boundJSONValues(ctx context.Context, raw []byte) error {
	decoder := jsontext.NewDecoder(bytes.NewReader(raw))
	remaining := maxValues
	for {
		if err := canceled(ctx); err != nil {
			return err
		}
		token, err := decoder.ReadToken()
		if errors.Is(err, io.EOF) {
			return canceled(ctx)
		}
		if err != nil {
			return problem(ErrInvalid, "encoding", "/", err)
		}
		if token.Kind() != '}' && token.Kind() != ']' {
			remaining--
			if remaining < 0 {
				return problem(ErrLimit, "values", "/")
			}
		}
	}
}

type boundedJSON struct {
	ctx     context.Context
	maximum int
	bytes.Buffer
}

func (buffer *boundedJSON) Write(data []byte) (int, error) {
	if err := canceled(buffer.ctx); err != nil {
		return 0, err
	}
	if len(data) > buffer.maximum-buffer.Len() {
		return 0, problem(ErrLimit, "bytes", "/")
	}
	return buffer.Buffer.Write(data)
}

func marshal(ctx context.Context, value any, maximum int) ([]byte, error) {
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	buffer := &boundedJSON{ctx: ctx, maximum: maximum}
	if err := json.MarshalWrite(buffer, value); err != nil {
		if canceledError := canceled(ctx); canceledError != nil {
			return nil, problem(ErrCanceled, "canceled", "/", canceledError, err)
		}
		if errors.Is(err, ErrLimit) {
			return nil, problem(ErrLimit, "bytes", "/", err)
		}
		return nil, problem(ErrInvalid, "encoding", "/", err)
	}
	return buffer.Bytes(), nil
}

func canonical(ctx context.Context, value any, maximum int) ([]byte, error) {
	raw, err := marshal(ctx, value, maximum)
	if err != nil {
		return nil, err
	}
	encoded := jsontext.Value(raw)
	if err := encoded.Canonicalize(); err != nil {
		return nil, problem(ErrInvalid, "encoding", "/", err)
	}
	if err := canceled(ctx); err != nil {
		return nil, err
	}
	if len(encoded) > maximum {
		return nil, problem(ErrLimit, "bytes", "/")
	}
	return []byte(encoded), nil
}

// This walk bounds concrete authoring data before native encoding can allocate or
// follow cyclic schema pointers. It does not infer workflow semantics from Go.
func boundAuthoring(ctx context.Context, declaration Declaration) error {
	values, text := maxValues, MaxBundleBytes
	var visit func(reflect.Value, int) error
	visit = func(value reflect.Value, depth int) error {
		if err := canceled(ctx); err != nil {
			return err
		}
		values--
		if values < 0 || depth > 128 {
			return problem(ErrLimit, "authoring_structure", "/")
		}
		switch value.Kind() {
		case reflect.Pointer:
			if !value.IsNil() {
				return visit(value.Elem(), depth+1)
			}
		case reflect.String:
			length := value.Len()
			text -= length
			if length > 4096 || text < 0 {
				return problem(ErrLimit, "authoring_text", "/")
			}
		case reflect.Struct:
			for index := 0; index < value.NumField(); index++ {
				if err := visit(value.Field(index), depth+1); err != nil {
					return err
				}
			}
		case reflect.Map:
			if value.Len() > values/2 {
				return problem(ErrLimit, "authoring_structure", "/")
			}
			iter := value.MapRange()
			for iter.Next() {
				if err := visit(iter.Key(), depth+1); err != nil {
					return err
				}
				if err := visit(iter.Value(), depth+1); err != nil {
					return err
				}
			}
		case reflect.Slice:
			if value.Len() > values {
				return problem(ErrLimit, "authoring_structure", "/")
			}
			for index := 0; index < value.Len(); index++ {
				if err := visit(value.Index(index), depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return visit(reflect.ValueOf(declaration), 0)
}

func boundDepth(ctx context.Context, value any, depth, maximum int) error {
	if err := canceled(ctx); err != nil {
		return err
	}
	switch value := value.(type) {
	case map[string]any:
		if depth > maximum {
			return problem(ErrLimit, "depth", "/")
		}
		for _, child := range value {
			if err := boundDepth(ctx, child, depth+1, maximum); err != nil {
				return err
			}
		}
	case []any:
		if depth > maximum {
			return problem(ErrLimit, "depth", "/")
		}
		for _, child := range value {
			if err := boundDepth(ctx, child, depth+1, maximum); err != nil {
				return err
			}
		}
	}
	return nil
}
