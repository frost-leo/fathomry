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

package trino_test

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"strings"
	"testing"

	trino "github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func TestOversizedSettingsPreparationRetainsNoOwnership(t *testing.T) {
	peer := newWirePeer(t, func(http.ResponseWriter, *http.Request, []byte) {
		t.Error("rejected settings dispatched application SQL")
	})
	settings := peer.settings()
	policy, err := trino.Recommend(settings)
	if err != nil {
		t.Fatal(err)
	}
	operationRuntime, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	defer operationRuntime.Close(context.Background())
	inbox, err := adapters.NewInbox[trino.Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	settings.User = strings.Repeat("x", 8<<20)
	beforeInput := settings
	for _, route := range []struct {
		name string
		run  func() error
	}{
		{"validate", func() error { return trino.Validate(settings) }},
		{"configuration", func() error { _, err := trino.Configuration(settings); return err }},
		{"recommend", func() error { _, err := trino.Recommend(settings); return err }},
		{"open", func() error {
			owner, err := trino.Open(context.Background(), settings, trino.Dependencies{Runtime: operationRuntime, Evidence: inbox})
			if owner != nil {
				_ = owner.Close(context.Background())
				return errors.New("rejected settings retained a source owner")
			}
			return err
		}},
	} {
		t.Run(route.name, func(t *testing.T) {
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			err := route.run()
			runtime.ReadMemStats(&after)
			if !errors.Is(err, trino.ErrInput) {
				t.Fatal("oversized settings lost their public input error", err)
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("allocated_bytes=%d", allocated)
			if allocated > 1<<20 {
				t.Errorf("rejected settings allocated %d bytes before their size check", allocated)
			}
			work, _ := operationRuntime.Inspect()
			evidence, _ := inbox.Inspect()
			if work.Active != 0 || work.WorkBytes != 0 || evidence.Outstanding != 0 || evidence.Bytes != 0 || peer.readiness.Load() != 0 || peer.posts.Load() != 0 {
				t.Fatal("offline rejection performed native I/O or retained work/evidence")
			}
			if settings != beforeInput {
				t.Fatal("preparation mutated caller settings")
			}
		})
	}
}
