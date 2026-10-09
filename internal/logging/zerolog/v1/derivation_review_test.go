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

package zerolog

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime"
	"strings"
	"testing"
)

func TestDerivedRefusalDoesNotCopyPayload(t *testing.T) {
	const limit = 1 << 20
	payload := strings.Repeat("x", limit-1024)
	for _, reason := range []string{"closed", "count", "bytes"} {
		t.Run(reason, func(t *testing.T) {
			fixture := bindFixture(t, OptionsV1{Name: "derived-copy", MaxRecordBytes: limit, Sinks: []SinkV1{{Name: "out", Writer: io.Discard}}}, 1)
			want := ErrLimit
			switch reason {
			case "closed":
				if err := fixture.assembly.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				want = ErrState
			case "count":
				for range MaxDerivedViews {
					if _, err := fixture.logger.With(); err != nil {
						t.Fatal(err)
					}
				}
			case "bytes":
				for {
					_, err := fixture.logger.With(slog.String("payload", payload))
					if errors.Is(err, ErrLimit) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			for range 16 {
				if view, err := fixture.logger.With(slog.String("payload", payload)); view != nil || !errors.Is(err, want) {
					t.Fatal("invalid derivation admission", err)
				}
			}
			runtime.ReadMemStats(&after)
			if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 1<<20 {
				t.Fatalf("refused derivations copied payload before admission: %d bytes", allocated)
			}
			runtime.KeepAlive(payload)
		})
	}
}
