/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package configuration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
)

func TestApplicationDeclarationOwnership(t *testing.T) {
	const owner = "github.com/frost-leo/fathomry/framework/configuration/v1"
	for _, kind := range []reflect.Type{reflect.TypeFor[Schema[application]](), reflect.TypeFor[Variable](), reflect.TypeFor[Encoding]()} {
		if kind.PkgPath() != owner {
			t.Fatal("application contract is owned by a technical source package", kind.PkgPath())
		}
	}
	if reflect.TypeFor[Schema[application]]() == reflect.TypeFor[configsource.Schema[application]]() ||
		reflect.TypeFor[Variable]() == reflect.TypeFor[configsource.Variable]() {
		t.Fatal("application declaration is only a technical alias")
	}
	secret := "private-schema-default"
	declaration := Schema[application]{Version: 1, Defaults: application{Defaults: map[string]string{"secret": secret}}}
	for _, value := range []any{declaration, Variable{Value: secret, Present: true}} {
		var output strings.Builder
		slog.New(slog.NewJSONHandler(&output, nil)).Info("declaration", "value", value)
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value)+output.String(), secret) {
			t.Fatal("declaration diagnostics leaked values")
		}
	}
	if _, err := json.Marshal(declaration); !errors.Is(err, ErrSerialization) {
		t.Fatal("schema serialized", err)
	}
	if err := json.Unmarshal([]byte("{}"), &declaration); !errors.Is(err, ErrSerialization) {
		t.Fatal("schema reconstructed", err)
	}
}

func TestApplicationVariableTranslation(t *testing.T) {
	for _, test := range []struct {
		name      string
		variables []Variable
		workers   int
		valid     bool
		acquired  bool
	}{
		{"present_json", []Variable{{Path: "/workers", Value: "3", Present: true, JSON: true}}, 3, true, true},
		{"absent", []Variable{{Path: "/workers", Value: "not-json", JSON: true}}, 2, true, true},
		{"explicit_zero", []Variable{{Path: "/workers", Value: "0", Present: true, JSON: true}}, 0, false, true},
		{"text_for_number", []Variable{{Path: "/workers", Value: "3", Present: true}}, 0, false, false},
		{"absent_unknown_path", []Variable{{Path: "/not_declared"}}, 0, false, false},
		{"duplicate_unset", []Variable{{Path: "/workers", JSON: true}, {Path: "/workers", JSON: true}}, 0, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependencies, inbox := localDependencies(t, 8)
			path := startupFiles(t, false)
			bootstrap, err := LoadBootstrap(context.Background(), BootstrapInput{Path: path}, dependencies.Local)
			if err != nil {
				t.Fatal(err)
			}
			session, err := LoadConfiguration(context.Background(), bootstrap, schema(), test.variables, dependencies)
			if session != nil {
				defer closeSession(t, session, !test.valid)
			}
			if (err == nil) != test.valid {
				t.Fatal("variable policy changed", err)
			}
			if (session != nil) != test.acquired {
				t.Fatal("invalid declaration caused source effects", err)
			}
			if test.valid && read(t, session.Reader()).Workers != test.workers {
				t.Fatal("variable translation changed value or presence")
			}
			if !test.acquired {
				evidence, err := inbox.Inspect()
				if err != nil || evidence.Outstanding != 1 {
					t.Fatal("rejected variable acquired a source", err)
				}
			}
		})
	}
}
