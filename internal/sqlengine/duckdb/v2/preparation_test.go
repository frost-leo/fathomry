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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestResolvedPreparationIsPureAndAuthoritative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-opened.duckdb")
	layer := resource.Layer{Kind: resource.Local, Content: []byte(`{"connections":2,"queued_calls":3,"result_bytes":1024,"reader_chunk_rows":7,"reader_chunk_bytes":4096,"reader_total_rows":70001,"reader_lifetime_ns":1000000000}`)}
	prepared, err := PrepareV1(OptionsV1{Name: "gh109", Path: path}, layer)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pure preparation opened database", err)
	}
	options, budget, limits := prepared.Options(), prepared.Budget(), prepared.Limits()
	if options.Connections != 2 || options.QueuedCalls != 3 || options.ResultBytes != 1024 || options.ReaderChunkRows != 7 || options.ReaderChunkBytes != 4096 || options.ReaderTotalRows != 70001 || options.ReaderLifetime != time.Second {
		t.Fatal("resolved fields lost")
	}
	if budget.WorkBytes != prepared.config.reservation() || budget.EvidenceBytes != prepared.config.evidenceReservation() ||
		budget.ReaderWorkBytes != prepared.config.readerReservation() || budget.ReaderEvidenceBytes != prepared.config.readerEvidenceReservation() ||
		limits.Bytes != int64(options.Connections)*max(budget.WorkBytes, budget.ReaderWorkBytes) || limits.MaxLeases != 2 ||
		budget.ReaderTerminalBytes < options.ReaderChunkBytes || budget.SourceBytes < options.MemoryBytes {
		t.Fatal("effective construction and budget diverge")
	}
	clear(layer.Content)
	selected := resource.WithLimits(prepared.Select(), limits)
	assembly, err := resource.Assemble(deadline(t), deadline(t), "gh109-resolved", selected)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := assembly.Close(deadline(t)); err != nil {
			t.Error(err)
		}
	}()
	source, info, err := resource.Bind(assembly, selected)
	if err != nil || !reflect.DeepEqual(source.owner.config, prepared.config) || info.Configuration.Revision != prepared.prepared.Description().Revision {
		t.Fatal("construction did not use frozen preparation", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("actual assembly did not open database", err)
	}
	options.Path = "modified"
	if prepared.Options().Path != path {
		t.Fatal("options mutated frozen configuration")
	}
	exact, err := PrepareResolvedV1(prepared.Options())
	if err != nil || exact.Budget() != prepared.Budget() || exact.Limits() != prepared.Limits() {
		t.Fatal("resolved validation diverged", err)
	}
	options = prepared.Options()
	options.Connections = 0
	if _, err := PrepareResolvedV1(options); err == nil {
		t.Fatal("resolved explicit zero was silently defaulted")
	}
	if _, err := PrepareResolvedV1(OptionsV1{Name: "gh109"}); err == nil {
		t.Fatal("absent resolved bounds accepted")
	}
}

func TestReaderPreparationRejectsMalformedBounds(t *testing.T) {
	for _, overlay := range []string{
		`{"reader_chunk_rows":0}`, `{"reader_chunk_rows":8193}`, `{"reader_chunk_rows":1.5}`,
		`{"reader_chunk_bytes":1023}`, `{"reader_chunk_bytes":67108865}`, `{"reader_total_rows":0}`,
		`{"reader_total_rows":1000000001}`, `{"reader_total_bytes":1023}`, `{"reader_total_bytes":1099511627777}`,
		`{"reader_lifetime_ns":0}`, `{"reader_lifetime_ns":3600000000001}`, `{"unknown_reader":true}`,
		`{"connections":0}`, `{"result_bytes":0}`, `{"reader_total_rows":null}`,
	} {
		if _, err := PrepareV1(OptionsV1{Name: "gh109"}, resource.Layer{Kind: resource.Local, Content: []byte(overlay)}); err == nil {
			t.Fatal("invalid explicit overlay accepted", overlay)
		}
	}
	prepared, err := PrepareV1(OptionsV1{Name: "gh109", QueuedCalls: 2}, resource.Layer{Kind: resource.Local, Content: []byte(`{"queued_calls":0,"path":""}`)})
	if err != nil || prepared.Options().QueuedCalls != 0 || prepared.Options().ReaderTotalRows != 1_000_000 {
		t.Fatal("absent/default/zero semantics changed", err)
	}
	if (Preparation{}).Budget() != (Budget{}) || (Preparation{}).Limits() != (resource.Limits{}) {
		t.Fatal("zero preparation has authority")
	}
}

func FuzzReaderPreparation(f *testing.F) {
	for _, data := range []string{`{}`, `{"reader_total_rows":70001}`, `{"reader_chunk_bytes":9223372036854775807}`, `{"reader_chunk_rows":0}`} {
		f.Add(data)
	}
	f.Fuzz(func(t *testing.T, data string) {
		if len(data) > 4096 {
			return
		}
		prepared, err := PrepareV1(OptionsV1{Name: "fuzz"}, resource.Layer{Kind: resource.Local, Content: []byte(data)})
		if err != nil {
			return
		}
		budget, limits := prepared.Budget(), prepared.Limits()
		if budget.WorkBytes <= 0 || budget.ReaderWorkBytes <= 0 || budget.EvidenceBytes <= 0 || budget.ReaderEvidenceBytes <= 0 ||
			limits.Bytes < int64(budget.Active)*budget.ReaderWorkBytes || limits.MaxLeases != 2 {
			t.Fatal("accepted overflowing budget")
		}
	})
}

func FuzzReaderFiniteBoundaries(f *testing.F) {
	f.Add(uint8(5), uint8(2), uint8(5))
	f.Add(uint8(0), uint8(1), uint8(1))
	f.Add(uint8(12), uint8(7), uint8(10))
	f.Fuzz(func(t *testing.T, totalRows, chunkRows, maxRows uint8) {
		rows, chunk, bound := int64(totalRows%33), int(chunkRows%8)+1, int64(maxRows%32)+1
		fixture := openFixture(t, OptionsV1{ReaderChunkRows: chunk, ReaderTotalRows: bound})
		reader, _ := startReader(t, fixture, deadline(t), deadline(t), fmt.Sprintf("SELECT i FROM range(%d) r(i) ORDER BY i", rows))
		var count int64
		for range 35 {
			result := nextReader(t, fixture, reader)
			progress := result.Outcome.Value.Snapshot()
			if progress.Reader.Offset != count || progress.Reader.Rows > chunk {
				t.Fatal("range mismatch")
			}
			for _, row := range progress.Steps[0].Rows {
				if row[0] != count {
					t.Fatal("value mismatch")
				}
				count++
			}
			if progress.Reader.Closed {
				if count != min(rows, bound) || progress.Steps[0].Complete != (rows <= bound) || (rows > bound && !errors.Is(result.Err(), ErrLimit)) {
					t.Fatal("boundary mismatch")
				}
				return
			}
		}
		t.Fatal("bounded read did not terminate")
	})
}
