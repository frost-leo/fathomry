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

package framework

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type logSettings struct {
	Application struct {
		I18n i18n.Preferences `json:"i18n"`
	} `json:"application"`
}
type forbiddenError struct{}

func (*forbiddenError) Error() string { panic("must not format unknown native error") }
func TestErrorLog(t *testing.T) {
	catalog, err := i18n.Prepare(CoreComponents()...)
	if err != nil {
		t.Fatal(err)
	}
	store := settings.NewStore[logSettings]()
	publish := func(locale string) settings.View {
		value := logSettings{}
		value.Application.I18n.Locale = locale
		snapshot, err := settings.New(value, func(value logSettings) logSettings { return value })
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Publish(snapshot); err != nil {
			t.Fatal(err)
		}
		return snapshot.View()
	}
	first := publish("en")
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	live, err := presenter.WithSettings(store.Reader())
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	ref, err := resource.Bind(scope, resource.Binding[int, i18n.Presenter]{
		Name: "presentation", Policy: resource.Fixed,
		Select: func(settings.View) (int, error) { return 1, nil }, Clone: func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[i18n.Presenter], error) {
			return &resource.Instance[i18n.Presenter]{Value: live}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	logger, err := NewErrorLog(slog.New(slog.NewJSONHandler(&output, nil)), presenter).WithResource(ref)
	if err != nil {
		t.Fatal(err)
	}
	marker := errors.New("private-operational-cause")
	original := fail(ErrDelivery, "example", marker)
	for _, locale := range []string{"en", "zh-CN"} {
		publish(locale)
		output.Reset()
		result := logger.Emit(context.Background(), original)
		if !result.Recognized || result.Issue != nil || !errors.Is(result.Presented, marker) || !errors.Is(result.Presented, ErrDelivery) {
			t.Fatal("presentation rewrote operation evidence")
		}
		var record struct {
			Error struct{ Code, Locale, Message string }
		}
		if err := json.Unmarshal([]byte(output.String()), &record); err != nil {
			t.Fatal(err)
		}
		if record.Error.Code != ErrDelivery.String() || record.Error.Locale != locale || record.Error.Message == "" || strings.Contains(output.String(), "private-operational-cause") {
			t.Fatal("unsafe or stale locale output")
		}
		explanation, found, err := catalog.Explain(ErrDelivery, locale)
		if err != nil || !found || explanation.Message.Text != record.Error.Message {
			t.Fatal("offline atlas differs")
		}
	}
	status, err := ref.Inspect()
	if err != nil || status.Generation != 1 {
		t.Fatal("Fixed presenter instance reconstructed")
	}
	output.Reset()
	unknown := &forbiddenError{}
	result := logger.Emit(context.Background(), unknown)
	if result.Recognized || result.Presented != unknown || !strings.Contains(output.String(), "native details withheld") {
		t.Fatal("unknown error formatted or rewritten")
	}
	if err := scope.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	result = logger.Emit(context.Background(), original)
	if result.Issue == nil || !errors.Is(result.Presented, marker) || !result.Recognized {
		t.Fatal("failed borrow replaced operational error")
	}
	if strings.Contains(output.String(), "private-operational-cause") {
		t.Fatal("fallback leaked cause")
	}
	output.Reset()
	if result := logger.Emit(nil, original); !errors.Is(result.Issue, ErrOptions) || output.Len() != 0 {
		t.Fatal("nil context admitted")
	}
	if result := logger.Emit(context.Background(), nil); result.Presented != nil || output.Len() != 0 {
		t.Fatal("nil error emitted")
	}
}
func TestLogMissingCatalogPreservesError(t *testing.T) {
	native := errors.New("private-native")
	original, err := failure.New(Definitions()[0], failure.Location{Operation: "example"}, native)
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	logger := NewErrorLog(slog.New(slog.NewJSONHandler(&output, nil)), i18n.Presenter{})
	result := logger.Emit(context.Background(), original)
	if result.Issue == nil || !errors.Is(result.Presented, native) || strings.Contains(output.String(), "private-native") {
		t.Fatal("localization failure lost error/privacy")
	}
	for _, definition := range Definitions() {
		if !definition.Valid() || definition.Code.Domain() != failure.DomainCore {
			t.Fatal("invalid assembly allocation")
		}
	}
}
