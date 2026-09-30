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

package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type project struct {
	Amount int      `json:"amount"`
	Labels []string `json:"labels"`
}

func TestExternalUse(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	scope, err := resource.New(ctx, resource.Options{Name: "consumer"})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	options := resource.Binding[int, int]{
		Name: "primary", Policy: resource.Follow,
		Select: func(view settings.View) (int, error) {
			snapshot, err := settings.As[project](view)
			if err != nil {
				return 0, err
			}
			value, err := snapshot.ValueCopy()
			return value.Amount, err
		},
		Clone: func(value int) int { return value },
		Equal: func(left, right int) bool { return left == right },
		Build: func(context.Context, int) (*resource.Instance[int], error) {
			return &resource.Instance[int]{Value: 42}, nil
		},
	}
	ref, err := resource.Bind(scope, options)
	if err != nil {
		t.Fatal(err)
	}
	views := make(chan settings.View, 1)
	watch, err := scope.Watch(ctx, views)
	if err != nil {
		t.Fatal(err)
	}
	data, err := settings.New(project{Amount: 7, Labels: []string{"owned"}}, func(value project) project { value.Labels = append([]string(nil), value.Labels...); return value })
	if err != nil {
		t.Fatal(err)
	}
	views <- data.View()
	report, err := watch.Next(ctx)
	if err != nil || report.Err != nil {
		t.Fatal("configuration handoff failed", err)
	}
	if err := report.Update.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	lease, err := ref.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	value, err := lease.Value()
	if err != nil || value != 42 {
		t.Fatal("typed external use failed")
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(lease.Release(), resource.ErrLease) {
		t.Fatal("duplicate release not detected")
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "resource", BaseLocale: "en", Resources: resource.Resources(), Directory: "resources", Definitions: resource.Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	explanation, found, err := catalog.Explain(resource.ErrLease, "zh-CN")
	if err != nil || !found || explanation.Message.Text == "" || explanation.Definition.Code.Domain() != failure.DomainCore {
		t.Fatal("resource atlas missing")
	}
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
