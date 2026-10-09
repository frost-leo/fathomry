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

package otel_test

import (
	"context"
	"errors"
	"testing"
	"time"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestStableDestinationIsRoutingNotReadinessOrOwnership(t *testing.T) {
	prepared, err := otel.Prepare(otel.Settings{Name: "routing", Version: 1, ServiceName: "routing", LogsEndpoint: "http://127.0.0.1:1/v1/logs"})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[otel.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{}, func(context.Context, adapters.Snapshot[otel.Result]) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	deps := otel.Dependencies{Runtime: runtime, Evidence: inbox}
	owner, err := prepared.Open(context.Background(), deps)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resource.New(context.Background(), resource.Options{Name: "routing"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, close := range []func(context.Context) error{scope.Close, owner.Close, runtime.Close, receiver.Finish} {
			if err := close(ctx); err != nil {
				t.Error(err)
			}
		}
	})
	if (*otel.Client)(nil).StableDestination() || new(otel.Client).StableDestination() || !owner.Client().StableDestination() {
		t.Fatal("zero/direct routing classification is incorrect")
	}
	alias, err := owner.Client().WithID("alias")
	if err != nil || !alias.StableDestination() {
		t.Fatal("correlation alias lost routing identity", err)
	}
	clients := make(map[resource.Policy]*otel.Client)
	for _, test := range []struct {
		name   string
		policy resource.Policy
		stable bool
	}{{"fixed", resource.Fixed, true}, {"follow", resource.Follow, false}} {
		ref, err := resource.Bind(scope, resource.Binding[int, otel.Handle]{Name: test.name, Policy: test.policy,
			Select: func(settings.View) (int, error) { return 0, nil }, Clone: func(value int) int { return value }, Equal: func(left, right int) bool { return left == right },
			Build: func(context.Context, int) (*resource.Instance[otel.Handle], error) {
				return nil, errors.New("routing inspection must not construct")
			}})
		if err != nil {
			t.Fatal(err)
		}
		client, err := otel.Using(context.Background(), ref, policy.Budget, deps)
		if err != nil || client.StableDestination() != test.stable {
			t.Fatal("binding routing classification is incorrect", err)
		}
		status, err := ref.Inspect()
		if err != nil || status.Active || status.Borrowers != 0 || status.Preparing {
			t.Fatal("routing inspection acquired a source or lease", err)
		}
		clients[test.policy] = client
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if !alias.StableDestination() || !clients[resource.Fixed].StableDestination() || clients[resource.Follow].StableDestination() {
		t.Fatal("closure changed immutable routing facts")
	}
}
