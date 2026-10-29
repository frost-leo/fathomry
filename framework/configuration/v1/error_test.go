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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

func TestHistoricalDefinitionsAndPresentation(t *testing.T) {
	names := []string{"invalid_declaration", "source_failed", "required_document_missing", "invalid_observation", "invalid_handle", "closed", "wait_interrupted", "publication_failed", "limit_exceeded", "runtime_serialization", "cleanup_failed"}
	definitions := Definitions()
	if len(definitions) != len(names) {
		t.Fatal("historical definition inventory changed")
	}
	for index, definition := range definitions {
		expected := failure.ErrorPrefix | failure.Code(failure.FacilityConfiguration)<<16 | failure.Code(index+1)
		if definition.Code != expected || definition.Identifier != failure.Identifier("fathomry.configuration."+names[index]) ||
			definition.Module != "fathomry" || definition.Component != "configuration" || definition.Revision != 1 ||
			definition.Details.ID != "fathomry.configuration.details" || definition.Details.Version != 1 {
			t.Fatal("historical contract changed", index)
		}
		cause := errors.New("private-native-canary")
		problem := at(definition.Code, "fixture", -1, cause)
		if !errors.Is(problem, cause) || !errors.Is(problem, expected) {
			t.Fatal("cause or code lost")
		}
		if strings.Contains(fmt.Sprintf("%+v", problem), "private-native-canary") {
			t.Fatal("cause leaked")
		}
		var detailed *failure.Detailed[Details]
		if !errors.As(problem, &detailed) {
			t.Fatal("typed historical detail contract lost")
		}
		if value, ok := detailed.Details(); !ok || value.SourceIndex != -1 {
			t.Fatal("unknown source index changed")
		}
		selected := at(definition.Code, "fixture", 1)
		if !errors.As(selected, &detailed) {
			t.Fatal("selected slot details lost")
		}
		if value, ok := detailed.Details(); !ok || value.SourceIndex != 1 {
			t.Fatal("selected slot index changed")
		}
	}
	catalogs, err := framework.Prepare(append(i18n.CoreComponents(), Component())...)
	if err != nil {
		t.Fatal(err)
	}
	if catalogs.Messages == nil || catalogs.Errors == nil {
		t.Fatal("metadata missing")
	}
	for _, definition := range definitions {
		for _, locale := range []string{"en", "zh-CN"} {
			if value, found, err := catalogs.Messages.Explain(definition.Code, locale); err != nil || !found || value.Message.Locale != locale || value.Message.Text == "" {
				t.Fatal("missing localized error explanation", definition.Code, locale, err)
			}
		}
	}
}

func TestSafeDiagnosticsAndSerialization(t *testing.T) {
	sentinel := "private-data-canary"
	values := []any{
		Bootstrap{Environment: sentinel}, BootstrapInput{Environment: sentinel}, File{Path: sentinel},
		Options{Variables: []configsource.Variable{{Value: sentinel}}},
		Status{Latest: Update{Err: errors.New(sentinel)}}, Update{Err: errors.New(sentinel)}, Session[application]{}, Dependencies{},
	}
	for _, value := range values {
		if strings.Contains(fmt.Sprintf("%#v %+v %v", value, value, value), sentinel) {
			t.Fatal("format leaked")
		}
		var buffer bytes.Buffer
		slog.New(slog.NewJSONHandler(&buffer, nil)).Info("fixture", "value", value)
		if strings.Contains(buffer.String(), sentinel) {
			t.Fatal("log leaked")
		}
	}
	for _, value := range []any{Options{}, Dependencies{}, Update{}, Status{}, Session[application]{}} {
		if _, err := json.Marshal(value); !errors.Is(err, ErrSerialization) {
			t.Fatal("opaque value serialized", err)
		}
	}
	for _, value := range []any{&Options{}, &Dependencies{}, &Update{}, &Status{}, &Session[application]{}} {
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, ErrSerialization) {
			t.Fatal("opaque handle reconstruction accepted", err)
		}
	}
	raw, err := json.Marshal(Bootstrap{Environment: sentinel})
	if err != nil || !bytes.Contains(raw, []byte(sentinel)) {
		t.Fatal("deliberate loadable data serialization was removed")
	}
}

func FuzzBootstrapPreparation(f *testing.F) {
	f.Add([]byte(`{"version":1,"environment":"test","mode":"local","provider":"viper","documents":[{"kind":"base","encoding":"json"}],"local":{"paths":["/tmp/example"]}}`))
	f.Add([]byte(`{"version":2,"local":null}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 8192 {
			t.Skip()
		}
		prepared, err := configsource.Prepare(context.Background(), configsource.Schema[Bootstrap]{Version: 1, Validate: func(_ context.Context, value Bootstrap) error { return validateBootstrap(value) }},
			[]configsource.Layer{{Kind: configsource.Base, Encoding: configsource.JSON, Content: raw}})
		if err == nil {
			value, err := prepared.ValueCopy()
			if err != nil || validateBootstrap(value) != nil {
				t.Fatal("invalid accepted bootstrap", err)
			}
		}
	})
}
