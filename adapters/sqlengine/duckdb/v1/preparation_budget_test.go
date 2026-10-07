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

package duckdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	source "github.com/frost-leo/fathomry/internal/resource"
	native "github.com/frost-leo/fathomry/internal/sqlengine/duckdb/v2"
)

func TestReviewedOversizedBatchRejectedBeforeRowContainerAllocation(t *testing.T) {
	settings := Settings{Name: "review-input-budget", InputBytes: 1024, ResultBytes: 1024,
		ReaderChunkBytes: 1024, ReaderTotalBytes: 1024}
	prepared, err := native.PrepareV1(options(settings))
	if err != nil {
		t.Fatal(err)
	}
	config := prepared.Options()
	inputs := []Request{{Mode: ExecuteMany, SQL: "INSERT INTO unused VALUES (?)", Rows: make([][]any, 65536)}}
	check := func() {
		requests, err := inward(inputs, config)
		if requests != nil || !errors.Is(err, ErrLimit) {
			t.Fatal("oversized known input escaped byte preflight", err)
		}
	}
	check()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const repetitions = 32
	for range repetitions {
		check()
	}
	runtime.ReadMemStats(&after)
	bytesPerCall := (after.TotalAlloc - before.TotalAlloc) / repetitions
	// Caller input is outside the measurement. The old Rows clone alone allocated
	// 1.5 MiB per rejected call on amd64; this loose ceiling allows small diagnostics
	// and runtime noise but cannot hide allocating the full rejected batch.
	if bytesPerCall > 64<<10 {
		t.Fatalf("rejected 1 KiB request allocated its 65536-row container: %d bytes/call", bytesPerCall)
	}
	runtime.KeepAlive(inputs)
	t.Logf("known oversized batch rejected at %d allocated bytes/call", bytesPerCall)
}

func TestOversizedSettingsRejectedBeforePreparationAllocation(t *testing.T) {
	directory := t.TempDir()
	operations, err := adapters.New(context.Background(), adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := operations.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	inbox, err := adapters.NewInbox[Result](adapters.EvidenceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: operations, Evidence: inbox}
	for _, route := range []struct {
		name string
		call func(Settings) error
	}{
		{"validate", Validate},
		{"recommend", func(value Settings) error { _, err := Recommend(value); return err }},
		{"configuration", func(value Settings) error { _, err := Configuration(value); return err }},
		{"open", func(value Settings) error {
			owner, err := Open(context.Background(), value, dependencies)
			if owner != nil {
				_ = owner.Close(context.Background())
				return errors.New("invalid oversized settings acquired source ownership")
			}
			return err
		}},
	} {
		for _, size := range []int{8 << 20, 16 << 20} {
			t.Run(route.name+"/"+strconv.Itoa(size), func(t *testing.T) {
				selected := Settings{Name: "oversized-settings", Path: filepath.Join(directory, strings.Repeat("p", size))}
				original := selected
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				err := route.call(selected)
				runtime.ReadMemStats(&after)
				runtime.KeepAlive(selected)
				if !errors.Is(err, ErrInput) || !errors.Is(err, source.ErrConfiguration) || errors.Is(err, native.ErrInput) {
					t.Fatal("oversized preparation changed public/native configuration error identity", err)
				}
				if selected != original {
					t.Fatal("rejected preparation mutated caller settings")
				}
				// Inputs exist before measurement. This allows bounded preparation
				// metadata while rejecting any full copy of the oversized path.
				allocated := after.TotalAlloc - before.TotalAlloc
				t.Logf("path bytes=%d, preparation allocated bytes=%d", len(selected.Path), allocated)
				if allocated > 256<<10 {
					t.Errorf("oversized path allocated before refusal: %d bytes", allocated)
				}
				work, err := operations.Inspect()
				if err != nil || work.Active != 0 || work.Accepted != 0 || work.WorkBytes != 0 {
					t.Fatal("preparation refusal retained work", err)
				}
				custody, err := inbox.Inspect()
				if err != nil || custody.Outstanding != 0 || custody.Bytes != 0 {
					t.Fatal("preparation refusal retained evidence", err)
				}
				entries, err := os.ReadDir(directory)
				if err != nil || len(entries) != 0 {
					t.Fatal("pure preparation touched the native file directory", err)
				}
			})
		}
	}
}
