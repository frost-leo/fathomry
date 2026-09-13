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
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/resource"
)

func privateDir(t testing.TB) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func fileOptions(t testing.TB, compressed bool, backups int) OptionsV1 {
	t.Helper()
	return OptionsV1{Name: "files", MaxRecordBytes: 1024, Sinks: []SinkV1{
		{Name: "file", File: &FileOptionsV1{Directory: privateDir(t), MaxBytes: 1024, Backups: backups, Compress: compressed}}}}
}
func readFileRecords(t testing.TB, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasSuffix(path, ".gz") {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		data, err = io.ReadAll(io.LimitReader(reader, 1<<20))
		if err != nil {
			t.Fatal(err)
		}
		if err = reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return decodeRecords(t, data)
}
func retainedCalls(t testing.TB, directory string) []string {
	t.Helper()
	names, err := filepath.Glob(filepath.Join(directory, "log-*"))
	if err != nil {
		t.Fatal(err)
	}
	names = append(names, filepath.Join(directory, activeName))
	var calls []string
	for _, name := range names {
		for _, record := range readFileRecords(t, name) {
			calls = append(calls, record["correlation"].(map[string]any)["call"].(string))
		}
	}
	return calls
}
func TestSizeRotationRetentionGzipAndCleanRestart(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(strconv.FormatBool(compressed), func(t *testing.T) {
			options := fileOptions(t, compressed, 2)
			directory := options.Sinks[0].File.Directory
			var secondary bytes.Buffer
			options.Sinks = append(options.Sinks, SinkV1{Name: "second", Writer: &secondary})
			f := bindFixture(t, options, 1)
			for index := 1; index <= 5; index++ {
				result := emit(t, f, "event-"+strconv.Itoa(index), Info, strings.Repeat("x", 520))
				if result.Err() != nil || !result.Outcome.Value.SinksCopy()[0].Accepted {
					t.Fatal("rotated write failed", result.Err())
				}
				if index > 1 && !result.Outcome.Value.SinksCopy()[0].Rotated {
					t.Fatal("size rotation did not occur")
				}
				drain(t, f.inbox)
			}
			if got := retainedCalls(t, directory); !reflect.DeepEqual(got, []string{"event-3", "event-4", "event-5"}) {
				t.Fatal("retention lost or duplicated a retained event", got)
			}
			if len(decodeRecords(t, secondary.Bytes())) != 5 {
				t.Fatal("secondary sink lost rotated events")
			}
			receipt, err := f.logger.Sync(context.Background(), correlation("sync"))
			result := observed(t, receipt, err)
			if result.Err() != nil || !result.Outcome.Value.SinksCopy()[0].Synced || result.Attempts != (invocation.Attempts{}) {
				t.Fatal("owned sync not reported", result.Err())
			}
			drain(t, f.inbox)
			if err := f.assembly.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(filepath.Join(directory, lockName)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("clean close left ownership marker")
			}
			if receipt, err := f.logger.Log(context.Background(), correlation("closed"), Info, "closed"); receipt != nil || err == nil {
				t.Fatal("post-close write accepted")
			}
			restarted := bindFixture(t, options, 1)
			result = emit(t, restarted, "event-6", Info, strings.Repeat("x", 520))
			if result.Err() != nil || !result.Outcome.Value.SinksCopy()[0].Rotated {
				t.Fatal("restart failed to continue rotation", result.Err())
			}
			drain(t, restarted.inbox)
			if got := retainedCalls(t, directory); !reflect.DeepEqual(got, []string{"event-4", "event-5", "event-6"}) {
				t.Fatal("restart reused an archive name", got)
			}
			entries, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				info, err := entry.Info()
				if err != nil {
					t.Fatal(err)
				}
				if info.Mode().Perm() != 0600 {
					t.Fatal("log file permissions widened")
				}
				if info.Size() > 2*1024+64<<10 {
					t.Fatal("archive exceeded configured envelope")
				}
			}
		})
	}
}
func TestSameTimestampManualRotationsNeverReplaceArchives(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(strconv.FormatBool(compressed), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := fileOptions(t, compressed, 8)
				directory := options.Sinks[0].File.Directory
				f := bindFixture(t, options, 1)
				stamp := time.Now()
				for index := 1; index <= 6; index++ {
					result := emit(t, f, "same-"+strconv.Itoa(index), Info, "one")
					if result.Err() != nil {
						t.Fatal(result.Err())
					}
					drain(t, f.inbox)
					receipt, err := f.logger.Rotate(context.Background(), correlation("rotate"))
					result = observed(t, receipt, err)
					if result.Err() != nil || !result.Outcome.Value.SinksCopy()[0].Rotated {
						t.Fatal("manual rotation failed", result.Err())
					}
					drain(t, f.inbox)
				}
				if !time.Now().Equal(stamp) {
					t.Fatal("test did not hold timestamp constant")
				}
				if got := retainedCalls(t, directory); !reflect.DeepEqual(got, []string{"same-1", "same-2", "same-3", "same-4", "same-5", "same-6"}) {
					t.Fatal("same-timestamp archive loss", got)
				}
				receipt, err := f.logger.Rotate(context.Background(), correlation("empty"))
				result := observed(t, receipt, err)
				if result.Err() != nil || result.Outcome.Value.SinksCopy()[0].Rotated {
					t.Fatal("empty file rotated")
				}
				drain(t, f.inbox)
			})
		})
	}
}
func TestCompressionFailureIsVisibleKeepsOriginalAndOtherSinks(t *testing.T) {
	options := fileOptions(t, true, 2)
	directory := options.Sinks[0].File.Directory
	var healthy bytes.Buffer
	options.Sinks = append(options.Sinks, SinkV1{Name: "healthy", Writer: &healthy})
	f := bindFixture(t, options, 1)
	f.allowCloseError = true
	result := emit(t, f, "original", Info, strings.Repeat("a", 520))
	if result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
	obstruction := filepath.Join(directory, "."+archiveName(1, true)+".tmp")
	if err := os.Mkdir(obstruction, 0700); err != nil {
		t.Fatal(err)
	}
	result = emit(t, f, "failed", Info, strings.Repeat("b", 520))
	sinks := result.Outcome.Value.SinksCopy()
	if !errors.Is(result.Err(), ErrMaintenance) || sinks[0].Attempted || sinks[0].Accepted || !sinks[1].Accepted {
		t.Fatal("maintenance failure was hidden or blocked later sink", result.Err())
	}
	var original *os.PathError
	if !errors.As(result.Err(), &original) {
		t.Fatal("original filesystem cause lost")
	}
	conformance.Private(t, result.Err(), directory)
	drain(t, f.inbox)
	result = emit(t, f, "later", Info, "later")
	if !errors.Is(result.Err(), ErrState) || result.Outcome.Value.SinksCopy()[0].Attempted || !result.Outcome.Value.SinksCopy()[1].Accepted {
		t.Fatal("failed file silently resumed or healthy sink stopped")
	}
	drain(t, f.inbox)
	if got := retainedCalls(t, directory); !reflect.DeepEqual(got, []string{"original"}) {
		t.Fatal("compression failure damaged original", got)
	}
	if len(decodeRecords(t, healthy.Bytes())) != 3 {
		t.Fatal("healthy sink lost a log")
	}
	if err := f.assembly.Close(context.Background()); !errors.Is(err, ErrMaintenance) {
		t.Fatal("cleanup omitted required recovery evidence")
	}
	if _, err := os.Stat(filepath.Join(directory, lockName)); err != nil {
		t.Fatal("failed file lost recovery marker")
	}
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	assembly, err := resource.Assemble(context.Background(), context.Background(), "restart", selected)
	if err == nil || !errors.Is(err, ErrState) {
		t.Fatal("unreconciled directory silently reopened")
	}
	if assembly != nil {
		_ = assembly.Close(context.Background())
	}
}
func TestArchiveCollisionNeverOverwritesExistingFile(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(strconv.FormatBool(compressed), func(t *testing.T) {
			options := fileOptions(t, compressed, 4)
			directory := options.Sinks[0].File.Directory
			f := bindFixture(t, options, 1)
			f.allowCloseError = true
			if result := emit(t, f, "original", Info, "original"); result.Err() != nil {
				t.Fatal(result.Err())
			}
			drain(t, f.inbox)
			collision := filepath.Join(directory, archiveName(1, compressed))
			if err := os.WriteFile(collision, []byte("collision-canary"), 0600); err != nil {
				t.Fatal(err)
			}
			receipt, err := f.logger.Rotate(context.Background(), correlation("collision"))
			result := observed(t, receipt, err)
			if !errors.Is(result.Err(), os.ErrExist) || result.Outcome.Value.SinksCopy()[0].Rotated {
				t.Fatal("archive collision was accepted")
			}
			drain(t, f.inbox)
			got, err := os.ReadFile(collision)
			if err != nil || string(got) != "collision-canary" {
				t.Fatal("collision overwrote existing archive")
			}
			if records := readFileRecords(t, filepath.Join(directory, activeName)); len(records) != 1 || records[0]["message"] != "original" {
				t.Fatal("collision destroyed original")
			}
		})
	}
}
func TestRetentionFailureStopsGrowthAfterPreservingNewArchive(t *testing.T) {
	options := fileOptions(t, true, 1)
	directory := options.Sinks[0].File.Directory
	f := bindFixture(t, options, 1)
	f.allowCloseError = true
	for index := 1; index <= 2; index++ {
		if result := emit(t, f, "event-"+strconv.Itoa(index), Info, strings.Repeat("x", 520)); result.Err() != nil {
			t.Fatal(result.Err())
		}
		drain(t, f.inbox)
	}
	oldest := filepath.Join(directory, archiveName(1, true))
	preserved := filepath.Join(t.TempDir(), "preserved.gz")
	if err := os.Rename(oldest, preserved); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(oldest, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(oldest, "obstruction"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	result := emit(t, f, "not-written", Info, strings.Repeat("y", 520))
	if !errors.Is(result.Err(), ErrMaintenance) || result.Outcome.Value.SinksCopy()[0].Attempted {
		t.Fatal("retention failure allowed further growth")
	}
	drain(t, f.inbox)
	if records := readFileRecords(t, filepath.Join(directory, archiveName(2, true))); len(records) != 1 || records[0]["correlation"].(map[string]any)["call"] != "event-2" {
		t.Fatal("new archive lost after retention failure")
	}
	if records := readFileRecords(t, preserved); len(records) != 1 {
		t.Fatal("fault fixture did not preserve old content")
	}
	if info, err := os.Stat(filepath.Join(directory, activeName)); err != nil || info.Size() != 0 {
		t.Fatal("new event written despite maintenance refusal")
	}
}
func TestExclusiveDirectoriesAndPartialConstructionCleanup(t *testing.T) {
	options := fileOptions(t, false, 2)
	directory := options.Sinks[0].File.Directory
	f := bindFixture(t, options, 1)
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resource.Assemble(context.Background(), context.Background(), "second", selected)
	if err == nil || !errors.Is(err, ErrState) {
		t.Fatal("second owner accepted")
	}
	if second != nil {
		_ = second.Close(context.Background())
	}
	if _, err := os.Stat(filepath.Join(directory, lockName)); err != nil {
		t.Fatal("failed competitor removed live lock")
	}
	if result := emit(t, f, "still-owned", Info, "one"); result.Err() != nil {
		t.Fatal("original owner was damaged", result.Err())
	}
	drain(t, f.inbox)
	firstDir, badDir := privateDir(t), privateDir(t)
	if err := os.Chmod(badDir, 0755); err != nil {
		t.Fatal(err)
	}
	badOptions := OptionsV1{Name: "partial", Sinks: []SinkV1{
		{Name: "first", File: &FileOptionsV1{Directory: firstDir}},
		{Name: "second", File: &FileOptionsV1{Directory: badDir}}}}
	selected, err = Select(badOptions)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := resource.Assemble(context.Background(), context.Background(), "partial", selected)
	if err == nil || partial == nil {
		t.Fatal("bad second file did not fail assembly")
	}
	if _, _, bindErr := resource.Bind(partial, selected); bindErr == nil {
		t.Fatal("partially usable source escaped")
	}
	for _, status := range partial.Snapshot().Sources {
		if status.Pending || !status.Released {
			t.Fatal("partial construction leaked handles")
		}
	}
	if _, err := os.Stat(filepath.Join(firstDir, lockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("earlier file owner not cleaned up")
	}
	if data, err := os.ReadFile(filepath.Join(firstDir, activeName)); err != nil || len(data) != 0 {
		t.Fatal("empty construction effect was not retained truthfully")
	}
}
func TestUnsafeExistingArtifactsRefuseWithoutMutation(t *testing.T) {
	for _, name := range []string{"partial", "unknown", "symlink", "too-many", "policy", "stale-lock"} {
		t.Run(name, func(t *testing.T) {
			options := fileOptions(t, true, 2)
			directory := options.Sinks[0].File.Directory
			switch name {
			case "partial":
				if err := os.WriteFile(filepath.Join(directory, activeName), []byte("{partial"), 0600); err != nil {
					t.Fatal(err)
				}
			case "unknown":
				if err := os.WriteFile(filepath.Join(directory, "unknown"), []byte("canary"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				external := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(external, []byte("canary"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(external, filepath.Join(directory, activeName)); err != nil {
					t.Fatal(err)
				}
			case "too-many":
				for index := 1; index <= maxBackups+3; index++ {
					if err := os.WriteFile(filepath.Join(directory, archiveName(uint64(index), true)), nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "policy":
				if err := os.WriteFile(filepath.Join(directory, archiveName(1, false)), nil, 0600); err != nil {
					t.Fatal(err)
				}
			case "stale-lock":
				if err := os.WriteFile(filepath.Join(directory, lockName), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			selected, err := Select(options)
			if err != nil {
				t.Fatal(err)
			}
			assembly, err := resource.Assemble(context.Background(), context.Background(), "refuse", selected)
			if err == nil {
				t.Fatal("unsafe existing directory accepted")
			}
			if assembly != nil {
				_ = assembly.Close(context.Background())
			}
			after, err := os.ReadDir(directory)
			if err != nil {
				t.Fatal(err)
			}
			var beforeNames, afterNames []string
			for _, entry := range before {
				beforeNames = append(beforeNames, entry.Name())
			}
			for _, entry := range after {
				afterNames = append(afterNames, entry.Name())
			}
			if !slices.Equal(beforeNames, afterNames) {
				t.Fatal("refusal deleted or created unrelated artifacts")
			}
		})
	}
}

type closeSpy struct {
	bytes.Buffer
	closes int
}

func (spy *closeSpy) Close() error { spy.closes++; return nil }
func TestAllOwnedFilesCloseAfterOneFailureAndBorrowedWriterNeverCloses(t *testing.T) {
	firstDir, secondDir := privateDir(t), privateDir(t)
	borrowed := &closeSpy{}
	options := OptionsV1{Name: "cleanup", Sinks: []SinkV1{{Name: "first", File: &FileOptionsV1{Directory: firstDir}},
		{Name: "second", File: &FileOptionsV1{Directory: secondDir}}, {Name: "borrowed", Writer: borrowed}}}
	f := bindFixture(t, options, 1)
	f.allowCloseError = true
	source, _, err := resource.Bind(f.assembly, f.selected)
	if err != nil {
		t.Fatal(err)
	}
	first := source.owner.sinks[0].file.file
	second := source.owner.sinks[1].file.file
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	err = f.assembly.Close(context.Background())
	if !errors.Is(err, os.ErrClosed) {
		t.Fatal("original cleanup cause lost")
	}
	if _, err := first.Write([]byte("must-not-write")); !errors.Is(err, os.ErrClosed) {
		t.Fatal("later cleanup skipped after first failure")
	}
	if borrowed.closes != 0 {
		t.Fatal("borrowed writer was closed")
	}
	if err := f.assembly.Close(context.Background()); !errors.Is(err, os.ErrClosed) {
		t.Fatal("repeat close erased failure")
	}
	for _, status := range f.assembly.Snapshot().Sources {
		if !status.Quiescent || !status.Released || status.Pending {
			t.Fatal("completed local handle release not reported")
		}
	}
}

func TestFileMaintenanceDoesNotInventSDKAttempts(t *testing.T) {
	f := bindFixture(t, fileOptions(t, true, 2), 1)
	result := emit(t, f, "file", Info, "message")
	if result.Err() != nil || result.Attempts != (invocation.Attempts{}) || !result.Outcome.Value.SinksCopy()[0].Accepted {
		t.Fatal("file output evidence changed")
	}
	drain(t, f.inbox)
	receipt, err := f.logger.Sync(context.Background(), correlation("sync"))
	result = observed(t, receipt, err)
	if result.Err() != nil || result.Attempts != (invocation.Attempts{}) || !result.Outcome.Value.SinksCopy()[0].Synced {
		t.Fatal("file sync masqueraded as SDK attempts")
	}
	drain(t, f.inbox)
	for _, rotated := range []bool{true, false} {
		receipt, err = f.logger.Rotate(context.Background(), correlation("rotate"))
		result = observed(t, receipt, err)
		if result.Err() != nil || result.Attempts != (invocation.Attempts{}) || result.Outcome.Value.SinksCopy()[0].Rotated != rotated {
			t.Fatal("rotation/no-op evidence changed")
		}
		drain(t, f.inbox)
	}
}

type cancelAtCheck struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int32
	at     int32
}

func (ctx *cancelAtCheck) Err() error {
	if ctx.checks.Add(1) == ctx.at {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestCancellationDuringGzipPreservesOriginal(t *testing.T) {
	for _, checkpoint := range []int32{2, 5, 10} {
		t.Run(strconv.Itoa(int(checkpoint)), func(t *testing.T) {
			directory := privateDir(t)
			sink, err := openFile(fileSettings{Directory: directory, MaxBytes: 1 << 20, Backups: 2, Compress: true})
			t.Cleanup(func() {
				if sink != nil {
					_ = sink.close()
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			original := bytes.Repeat([]byte("test-owned-record\n"), 40000)
			count, err := sink.file.Write(original)
			if err != nil {
				t.Fatal(err)
			}
			sink.size = int64(count)
			base, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cancelAtCheck{Context: base, cancel: cancel, at: checkpoint}
			rotated, err := sink.rotate(ctx)
			if rotated || !errors.Is(err, context.Canceled) || sink.failed == nil {
				t.Fatal("mid-rotation cancellation lost fail-stop evidence", err)
			}
			after, err := os.ReadFile(filepath.Join(directory, activeName))
			if err != nil || !bytes.Equal(after, original) {
				t.Fatal("cancellation damaged original")
			}
			if err := sink.close(); !errors.Is(err, context.Canceled) {
				t.Fatal("close erased maintenance failure", err)
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 2 {
				t.Fatal("partial or unbounded temporary output remained", err)
			}
			if _, err := os.Stat(filepath.Join(directory, lockName)); err != nil {
				t.Fatal("recovery marker lost", err)
			}
		})
	}
}

func TestCanceledCloseContinuesWithoutErasingEarlierFailure(t *testing.T) {
	first, err := openFile(fileSettings{Directory: privateDir(t), MaxBytes: 1024, Backups: 2})
	t.Cleanup(func() {
		if first != nil {
			_ = first.close()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := openFile(fileSettings{Directory: privateDir(t), MaxBytes: 1024, Backups: 2})
	t.Cleanup(func() {
		if second != nil {
			_ = second.close()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.file.Close(); err != nil {
		t.Fatal(err)
	}
	owner := &outputs{sinks: []output{{file: first}, {file: second}}}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := owner.close(&cancelAtCheck{Context: base, cancel: cancel, at: 2})
	if result.Released || result.Quiescent || result.Continue == nil || !errors.Is(result.Err, os.ErrClosed) || !errors.Is(result.Err, context.Canceled) {
		t.Fatal("partial cleanup lost evidence", result.Err)
	}
	if first.closed || !second.closed {
		t.Fatal("cleanup continuation boundary changed")
	}
	finished := result.Continue(context.Background())
	if !finished.Released || !finished.Quiescent || finished.Err != nil || !first.closed {
		t.Fatal("pending cleanup did not finish", finished.Err)
	}
	if !errors.Is(result.Err, os.ErrClosed) {
		t.Fatal("historical failure changed after continuation")
	}
	if _, err := os.Stat(filepath.Join(second.settings.Directory, lockName)); err != nil {
		t.Fatal("failed close erased recovery marker")
	}
	if _, err := os.Stat(filepath.Join(first.settings.Directory, lockName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("healthy close retained marker")
	}
}

func TestCanceledMaintenanceDoesNotRotateOrBreakHealthyFile(t *testing.T) {
	options := fileOptions(t, true, 2)
	f := bindFixture(t, options, 1)
	if result := emit(t, f, "one", Info, "one"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	drain(t, f.inbox)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if receipt, err := f.logger.Rotate(ctx, correlation("canceled")); receipt != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled maintenance entered file operations")
	}
	if result := emit(t, f, "two", Info, "two"); result.Err() != nil {
		t.Fatal("untouched file failed after canceled operation", result.Err())
	}
	drain(t, f.inbox)
}
