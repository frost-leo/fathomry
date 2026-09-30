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

package resource_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type hostile struct{ calls *atomic.Int32 }

func (value hostile) String() string { value.calls.Add(1); return "private-runtime-canary" }
func (value hostile) Error() string  { return value.String() }
func (value hostile) MarshalJSON() ([]byte, error) {
	value.calls.Add(1)
	return []byte(`"private-runtime-canary"`), nil
}

func TestDiagnostics(t *testing.T) {
	var calls atomic.Int32
	private := hostile{calls: &calls}
	scope, err := resource.New(context.Background(), resource.Options{Name: "safe"})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	ref, err := resource.Bind(scope, resource.Binding[int, hostile]{
		Name:   "instance",
		Select: func(settings.View) (int, error) { return 1, nil },
		Clone:  func(value int) int { return value },
		Build: func(context.Context, int) (*resource.Instance[hostile], error) {
			return &resource.Instance[hostile]{Value: private}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := settings.New(1, func(value int) int { return value })
	update, err := scope.Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, _ := ref.Acquire(context.Background())
	defer lease.Release()
	watch, err := scope.Watch(context.Background(), make(chan settings.View))
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close(context.Background())
	status, _ := ref.Inspect()
	status.Err, status.CleanupErr = private, private
	values := []any{
		scope, *scope, ref, &ref, lease, &lease, update, *update, watch, *watch,
		resource.Instance[hostile]{Value: private}, resource.ReleaseResult{Err: private},
		resource.Binding[int, hostile]{Name: "safe"}, status, &status,
		resource.Observation{Err: private, Update: update},
	}
	for _, value := range values {
		text := fmt.Sprintf("%v %+v %#v %s %q", value, value, value, value, value)
		if stringer, ok := value.(fmt.Stringer); ok {
			text += stringer.String()
		}
		if stringer, ok := value.(fmt.GoStringer); ok {
			text += stringer.GoString()
		}
		if strings.Contains(text, "private-runtime-canary") {
			t.Fatal("fmt disclosed runtime data")
		}
		if _, err := json.Marshal(value); !errors.Is(err, resource.ErrSerialization) {
			t.Fatal("runtime value serialized", err)
		}
		var buffer bytes.Buffer
		slog.New(slog.NewJSONHandler(&buffer, nil)).Info("test", "value", value)
		if strings.Contains(buffer.String(), "private-runtime-canary") {
			t.Fatal("slog disclosed runtime data")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("diagnostics invoked native methods")
	}
	for _, value := range []slog.LogValuer{(*resource.Scope)(nil), (*resource.Ref[int])(nil), (*resource.Lease[int])(nil), (*resource.Instance[hostile])(nil), (*resource.Status)(nil)} {
		_ = value.LogValue()
	}
	for _, value := range []any{new(resource.Scope), new(resource.Ref[int]), new(resource.Lease[int]), new(resource.Instance[int]), new(resource.Update), new(resource.Watch), new(resource.Status)} {
		if err := json.Unmarshal([]byte("{}"), value); !errors.Is(err, resource.ErrSerialization) {
			t.Fatal("runtime reconstruction admitted")
		}
	}
}

func TestDefinitionsAndResources(t *testing.T) {
	definitions := resource.Definitions()
	if len(definitions) != 13 || resource.ErrOptions != 0xA0040001 || resource.ErrSerialization != 0xA004000D {
		t.Fatal("code allocation changed")
	}
	catalog, err := failure.Prepare(definitions...)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "resource", BaseLocale: "en", Resources: resource.Resources(), Directory: "resources", Definitions: definitions})
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range definitions {
		found, exists, err := catalog.Lookup(definition.Code)
		if err != nil || !exists || found != definition || definition.Code.Domain() != failure.DomainCore {
			t.Fatal("atlas owner or domain incorrect")
		}
		english, ok, err := messages.Explain(definition.Code, "en")
		if err != nil || !ok || english.Message.Text != definition.Message {
			t.Fatal("baseline differs")
		}
		chinese, ok, err := messages.Explain(definition.Code, "zh-CN")
		if err != nil || !ok || chinese.Message.Text == english.Message.Text {
			t.Fatal("translation missing")
		}
	}
	definitions[0].Message = "mutated"
	if resource.Definitions()[0].Message == "mutated" {
		t.Fatal("definitions alias escaped")
	}
	coverage, err := messages.Coverage("zh-CN")
	if err != nil || len(coverage) != 1 || len(coverage[0].Missing) != 0 {
		t.Fatal("resource coverage incomplete")
	}
}
