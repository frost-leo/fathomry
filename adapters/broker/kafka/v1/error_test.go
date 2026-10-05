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

package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/twmb/franz-go/pkg/kerr"
)

func TestErrorsResourcesAndPrivacy(t *testing.T) {
	catalog, err := failure.Prepare(Definitions()...)
	if err != nil || catalog == nil {
		t.Fatal(err)
	}
	translations, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "broker_kafka", BaseLocale: "en", Directory: "resources", Resources: Resources(), Definitions: Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range Definitions() {
		for _, locale := range []string{"en", "zh-CN"} {
			_, found, err := translations.Explain(definition.Code, locale)
			if err != nil || !found {
				t.Fatal("offline atlas incomplete", err)
			}
		}
	}
	canary := "payload-secret-canary"
	original := &kerr.Error{Message: canary, Code: 1234}
	wrapped := fail(ErrRead, "read", original)
	if got, ok := InspectError(wrapped); !ok || got != original {
		t.Fatal("native cause identity lost")
	}
	for _, value := range []any{Message{Topic: canary, Value: []byte(canary)}, Header{Key: canary}, Position{Topic: canary}, wrapped,
		Settings{User: canary, Password: canary}, &Owner{}, &Client{}, &Group{}, GroupSnapshot{MemberID: canary, Err: wrapped}, GroupBatch{}} {
		if strings.Contains(fmt.Sprintf("%+v %#v %s", value, value, value), canary) {
			t.Fatal("formatting leaked data")
		}
		var buffer bytes.Buffer
		slog.New(slog.NewJSONHandler(&buffer, nil)).Info("safe", "value", value)
		if strings.Contains(buffer.String(), canary) {
			t.Fatal("implicit JSON logging leaked data")
		}
	}
	for _, value := range []any{Message{Value: []byte(canary)}, &Owner{}, &Client{}, &Consumer{}, &Group{}, Result{}, GroupBatch{}} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime serialization accepted")
		}
	}
}
