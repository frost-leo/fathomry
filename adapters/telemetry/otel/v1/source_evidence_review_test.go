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
	"testing"
	"time"

	otel "github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestSourceCleanupEvidenceRetainsPublicSourceIdentity(t *testing.T) {
	config := otel.Settings{Name: "cleanup-identity", ServiceName: "fixture", LogsEndpoint: "http://127.0.0.1:1/logs"}
	policy, err := otel.Recommend(config)
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
	owner, err := otel.Open(context.Background(), config, otel.Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	defer func() { _ = owner.Close(ctx); _ = runtime.Close(ctx) }()
	expected := owner.Info()
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := delivery.Ack(); err != nil {
			t.Error(err)
		}
	}()
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil || snapshot.Err() != nil {
		t.Fatal(err, snapshot.Err())
	}
	result, present := snapshot.ValueCopy()
	if !present || result.HasData() || len(result.SignalsCopy()) != 0 {
		t.Fatal("final source evidence omitted its metadata or invented signal data")
	}
	source := result.Source()
	if source.Name != expected.Name || source.Provider != expected.Provider || source.Revision != expected.Revision || source.Scope != expected.Scope {
		t.Fatal("final source evidence lost physical source identity")
	}
	attribution := result.Attribution()
	if attribution.Sequence != snapshot.Info().Sequence || attribution.Operation != "telemetry.otel.open" || attribution.Source != (adapters.Source{}) {
		t.Fatal("source cleanup invented a borrow or lost original root attribution")
	}
}
