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

package i18n

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type projectSettings struct {
	Application struct {
		I18n Preferences `json:"i18n"`
	} `json:"application"`
}

func publishLocale(t testing.TB, store settings.Store[projectSettings], locale string) {
	t.Helper()
	var value projectSettings
	value.Application.I18n.Locale = locale
	snapshot, err := settings.New(value, func(value projectSettings) projectSettings { return value })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
}
func mustPresenter(t testing.TB, catalog *Catalog) Presenter {
	t.Helper()
	value, err := NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func mustOccurrence(t testing.TB) *failure.Error {
	t.Helper()
	value, err := failure.New(fixtureDefinition(), failure.Location{Operation: "read"}, &fs.PathError{Op: "read", Path: "private-native-canary", Err: fs.ErrPermission})
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func presented(t testing.TB, value error) *Presented {
	t.Helper()
	result, ok := value.(*Presented)
	if !ok {
		t.Fatal("missing presentation wrapper")
	}
	return result
}

type customOccurrence struct {
	core    *failure.Error
	public  string
	private string
	stamp   time.Time
}

func (value *customOccurrence) Failure() *failure.Error { return value.core }
func (value *customOccurrence) Unwrap() error           { return value.core }
func (*customOccurrence) Error() string                 { panic("original formatter must not run") }

func TestPresenter(t *testing.T) {
	t.Run("custom_data_settings_and_explicit_atlas", func(t *testing.T) {
		var projections atomic.Int32
		component := fixtureComponent()
		component.Bindings = []Binding{{Code: fixtureCode, Message: "example.source.details", MessageContract: "v1", Details: fixtureDefinition().Details, Project: func(value error) (Input, error) {
			projections.Add(1)
			occurrence, ok := value.(*customOccurrence)
			if !ok {
				return Input{}, errors.New("wrong occurrence type")
			}
			return Input{Arguments: []Argument{{Name: "label", Value: occurrence.public}}}, nil
		}}}
		catalog := mustCatalog(t, component)
		store := settings.NewStore[projectSettings]()
		publishLocale(t, store, "en")
		presenter, err := mustPresenter(t, catalog).WithSettings(store.Reader())
		if err != nil {
			t.Fatal(err)
		}
		core := mustOccurrence(t)
		native := core.Unwrap()[0]
		original := &customOccurrence{core: core, public: "public", private: "private-detail-canary", stamp: time.Now()}
		english := presented(t, presenter.Present(original))
		if english.Issue() != nil || english.Info().Message.Locale != "en" || !errors.Is(english, fixtureCode) || !errors.Is(english, native) {
			t.Fatal("runtime identity/locale lost")
		}
		var retained *customOccurrence
		if !errors.As(english, &retained) || retained != original || english.Failure() != core || errors.Unwrap(english) != original {
			t.Fatal("original custom occurrence not retained")
		}
		chinese, found, err := catalog.Explain(fixtureCode, "zh-CN")
		if err != nil || !found || chinese.Message.Locale != "zh-CN" || projections.Load() != 1 {
			t.Fatal("offline query used runtime settings/projector")
		}
		publishLocale(t, store, "zh-CN")
		translated := presented(t, presenter.Present(english))
		if translated.Info().Message.Locale != "zh-CN" || english.Info().Message.Locale != "en" || translated.Unwrap() != original || projections.Load() != 2 {
			t.Fatal("preference update changed history or re-presentation lost original")
		}
		checkPrivate(t, translated)
		publishLocale(t, store, "")
		fallback := presented(t, presenter.Present(original))
		if !errors.Is(fallback.Issue(), ErrPreferences) || fallback.Info().Message.Text != fixtureDefinition().Message || !errors.Is(fallback, native) {
			t.Fatal("invalid settings replaced the original error")
		}
		explicit, err := presenter.WithLocale("zh-CN")
		if err != nil {
			t.Fatal(err)
		}
		if got := presented(t, explicit.Present(original)); got.Issue() != nil || got.Info().Message.Locale != "zh-CN" {
			t.Fatal("explicit locale depended on invalid settings")
		}
		mismatch := fixtureDefinition()
		mismatch.Revision++
		different, err := failure.New(mismatch, failure.Location{})
		if err != nil {
			t.Fatal(err)
		}
		before := projections.Load()
		if got := presented(t, explicit.Present(different)); !errors.Is(got.Issue(), ErrBinding) || projections.Load() != before {
			t.Fatal("stale definition reached projector")
		}
	})
	t.Run("typed_details_and_projection_failures", func(t *testing.T) {
		type details struct {
			Label   string
			private time.Time
		}
		component := fixtureComponent()
		component.Bindings = []Binding{{Code: fixtureCode, Message: "example.source.details", MessageContract: "v1", Details: fixtureDefinition().Details, Project: func(value error) (Input, error) {
			occurrence, ok := value.(*failure.Detailed[details])
			if !ok {
				return Input{}, errors.New("not the direct typed occurrence")
			}
			fields, ok := occurrence.Details()
			if !ok {
				return Input{}, errors.New("missing detail")
			}
			return Input{Arguments: []Argument{{Name: "label", Value: fields.Label}}}, nil
		}}}
		original, err := failure.NewDetailed(fixtureDefinition(), failure.Location{}, details{Label: "public", private: time.Now()}, func(value details) details { return value }, fs.ErrPermission)
		if err != nil {
			t.Fatal(err)
		}
		presenter, err := mustPresenter(t, mustCatalog(t, component)).WithLocale("en")
		if err != nil {
			t.Fatal(err)
		}
		if got := presented(t, presenter.Present(original)); got.Issue() != nil || !errors.Is(got, fs.ErrPermission) {
			t.Fatal("arbitrary typed details could not project")
		}
		rejected := errors.New("private-projection-canary")
		for _, callback := range []func(error) (Input, error){
			func(error) (Input, error) { return Input{}, rejected },
			func(error) (Input, error) { panic("private-panic-canary") },
			func(error) (Input, error) { panic(nil) },
			func(error) (Input, error) {
				return Input{Arguments: []Argument{{Name: "label", Value: hostileScalar{}}}}, nil
			},
		} {
			component.Bindings[0].Project = callback
			presenter, _ = mustPresenter(t, mustCatalog(t, component)).WithLocale("en")
			got := presented(t, presenter.Present(original))
			if got.Issue() == nil || got.Info().Message.Text != fixtureDefinition().Message || !errors.Is(got, fs.ErrPermission) || errors.Is(got, ErrProjection) {
				t.Fatal("secondary failure became the operation result")
			}
			checkPrivate(t, got)
		}
		component.Bindings[0].Project = func(error) (Input, error) { return Input{}, rejected }
		presenter, _ = mustPresenter(t, mustCatalog(t, component)).WithLocale("en")
		if got := presented(t, presenter.Present(original)); !errors.Is(got.Issue(), rejected) {
			t.Fatal("projection native cause lost")
		}
	})
	t.Run("foreign_nil_and_invalid_handles", func(t *testing.T) {
		native := errors.New("private-native-canary")
		presenter := Presenter{}
		if presenter.Present(nil) != nil || presenter.Present(native) != native {
			t.Fatal("foreign/nil error rewritten")
		}
		original := mustOccurrence(t)
		wrapped := fmt.Errorf("foreign: %w", original)
		if presenter.Present(wrapped) != wrapped {
			t.Fatal("guessed descendant occurrence")
		}
		if got := presented(t, presenter.Present(original)); !errors.Is(got.Issue(), ErrCatalog) || got.Info().Message.Locale != "" {
			t.Fatal("unprepared presenter claimed a locale")
		}
		for _, handle := range []any{Presenter{}, Presented{}, Selection{}, Catalog{}} {
			if _, err := json.Marshal(handle); !errors.Is(err, ErrSerialization) {
				t.Fatal("runtime handle serialized")
			}
		}
		var missing *Presented
		if missing.Error() != "<nil>" || missing.Failure() != nil || missing.Unwrap() != nil || missing.Issue() != nil || missing.Info() != (Presentation{}) {
			t.Fatal("nil presentation semantics changed")
		}
		if _, err := presenter.WithLocale("en"); !errors.Is(err, ErrCatalog) {
			t.Fatal("zero presenter configured")
		}
		if _, err := presenter.Render("", nil, nil); !errors.Is(err, ErrCatalog) {
			t.Fatal("zero presenter rendered")
		}
	})
}
func checkPrivate(t testing.TB, value error) {
	t.Helper()
	var output bytes.Buffer
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		fmt.Fprintf(&output, format, value)
	}
	slog.New(slog.NewJSONHandler(&output, nil)).Info("failure", "error", value)
	for _, canary := range []string{"private-native-canary", "private-detail-canary", "private-projection-canary", "private-panic-canary", "PANIC"} {
		if strings.Contains(output.String(), canary) {
			t.Fatal("unsafe presentation disclosed data or invoked a formatter")
		}
	}
}

func TestApplicationPreferences(t *testing.T) {
	const helper = "FATHOMRY_I18N_DEFAULT_TEST"
	if os.Getenv(helper) != "child" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApplicationPreferences$", "-test.timeout=20s", "-test.v")
		command.Env = append(os.Environ(), helper+"=child", "GORACE="+os.Getenv("GORACE")+" atexit_sleep_ms=0")
		output, err := command.CombinedOutput()
		if err != nil || ctx.Err() != nil || !strings.Contains(string(output), "application locale contract completed") {
			t.Fatalf("locale child failed: %v\n%s", err, output)
		}
		return
	}
	catalog := mustCatalog(t, fixtureComponent())
	presenter := mustPresenter(t, catalog)
	original := mustOccurrence(t)
	early := presented(t, presenter.Present(original))
	if !errors.Is(early.Issue(), settings.ErrUnconfigured) || early.Info().Message.Text != fixtureDefinition().Message || !errors.Is(early, fs.ErrPermission) {
		t.Fatal("bootstrap failure lost")
	}
	if _, err := presenter.Render("example.source.failed", nil, nil); !errors.Is(err, ErrPreferences) {
		t.Fatal("plain rendering invented a default locale")
	}
	store := settings.NewStore[projectSettings]()
	publishLocale(t, store, "en")
	if err := settings.Configure(store.Reader()); err != nil {
		t.Fatal(err)
	}
	english := presented(t, presenter.Present(original))
	if english.Info().Message.Locale != "en" || english.Issue() != nil {
		t.Fatal("application default not used")
	}
	explicit, err := presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	if got := presented(t, explicit.Present(original)); got.Info().Message.Locale != "zh-CN" {
		t.Fatal("explicit query used the application language")
	}
	publishLocale(t, store, "zh-CN")
	if got := presented(t, presenter.Present(original)); got.Info().Message.Locale != "zh-CN" || english.Info().Message.Locale != "en" {
		t.Fatal("new preference did not preserve old presentation")
	}
	t.Log("application locale contract completed")
}
