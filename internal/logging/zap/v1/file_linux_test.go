//go:build linux

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

package zap

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sys/unix"
)

func TestBorrowedStreamChild(t *testing.T) {
	if os.Getenv("FATHOMRY_ZAP_STREAM_CHILD") != "1" {
		return
	}
	options := OptionsV1{Name: "streams", Outputs: []OutputV1{{Name: "out", Kind: "stdout"}, {Name: "err", Kind: "stderr"}}}
	before := sdk.L()
	fixture := bindFixture(t, options, nil, 2)
	if result := logResult(t, fixture.logger, "stream"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	receipt, err := fixture.logger.Sync(context.Background(), fault.Correlation{Call: "stream-sync"})
	result := resultOf(t, receipt, err)
	if !errors.Is(result.Err(), unix.EINVAL) {
		t.Fatal("pipe Sync limitation hidden")
	}
	if err := fixture.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stdout.WriteString("stdout-still-open\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stderr.WriteString("stderr-still-open\n"); err != nil {
		t.Fatal(err)
	}
	if sdk.L() != before {
		t.Fatal("global logger replaced")
	}
}
func TestNativeBorrowedStreamsAndSyncLimitations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBorrowedStreamChild$", "-test.timeout=15s")
	command.Env = append(os.Environ(), "FATHOMRY_ZAP_STREAM_CHILD=1", "GORACE=atexit_sleep_ms=0")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.Output()
	if err != nil || ctx.Err() != nil {
		t.Fatalf("stream child failed: %v\n%s", err, stdout)
	}
	for _, output := range [][]byte{stdout, stderr.Bytes()} {
		if bytes.Count(output, []byte("\"fathomry.call\":\"stream\"")) != 1 || !bytes.Contains(output, []byte("-still-open")) {
			t.Fatal("stream fan-out, diagnostic silence or borrowed ownership changed")
		}
	}
}

func archiveBytes(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(path, ".gz") {
		return data
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return decoded
}
func TestNativeFileRotationCompressionRetentionAndRestart(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(fmt.Sprint(compressed), func(t *testing.T) {
			options := fileOptions(t, compressed)
			sink := &recordingSink{}
			fixture := bindFixture(t, options, sink, 32)
			for index := range 12 {
				receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: fmt.Sprintf("call-%d", index)},
					zapcore.InfoLevel, fmt.Sprintf("record-%02d-%s", index, strings.Repeat("x", 600)))
				if result := resultOf(t, receipt, err); result.Err() != nil {
					t.Fatal(result.Err())
				}
			}
			if err := fixture.assembly.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if sink.count() != 12 {
				t.Fatal("secondary sink lost records during rotation")
			}
			directory := options.Outputs[0].Directory
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 4 {
				t.Fatal("retention count changed", err)
			}
			var records []string
			var disk int64
			for _, entry := range entries {
				data := archiveBytes(t, filepath.Join(directory, entry.Name()))
				info, err := entry.Info()
				if err != nil {
					t.Fatal(err)
				}
				disk += info.Size()
				if info.Mode().Perm() != 0600 || len(data) > 1024 || strings.HasSuffix(entry.Name(), ".tmp") {
					t.Fatal("file mode, size or completed maintenance changed")
				}
				for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
					for index := 8; index < 12; index++ {
						if bytes.Contains(line, fmt.Appendf(nil, "record-%02d-", index)) {
							records = append(records, fmt.Sprint(index))
						}
					}
				}
			}
			sort.Strings(records)
			if fmt.Sprint(records) != "[10 11 8 9]" || disk > 4*compressedLimit(1024) {
				t.Fatal("retention lost wrong records or exceeded file envelope")
			}
			reopened, err := openRotatingFile(defaults(options).Outputs[0])
			if err != nil {
				t.Fatal("clean restart failed", err)
			}
			if reopened.sequence != 11 {
				t.Fatal("restart forgot sequence")
			}
			if _, err := reopened.Write([]byte(strings.Repeat("y", 800) + "\n")); err != nil {
				t.Fatal(err)
			}
			if reopened.sequence != 12 {
				t.Fatal("restart did not advance collision-free sequence")
			}
			if err := reopened.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := reopened.Write([]byte("late\n")); !errors.Is(err, ErrState) {
				t.Fatal("post-Close write reopened file")
			}
		})
	}
}

func TestRapidRotationsPreserveEveryRecordWithoutTimeNames(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		options := defaults(fileOptions(t, compressed)).Outputs[0]
		options.MaxBackups = 64
		writer, err := openRotatingFile(options)
		if err != nil {
			t.Fatal(err)
		}
		for index := range 48 {
			data := []byte(fmt.Sprintf("%03d:%s\n", index, strings.Repeat("x", 700)))
			if _, err := writer.Write(data); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir(options.Directory)
		if err != nil || len(entries) != 48 {
			t.Fatal("rapid rotations collided", err)
		}
		found := make(map[string]int)
		for _, entry := range entries {
			data := archiveBytes(t, filepath.Join(options.Directory, entry.Name()))
			found[string(data[:3])]++
		}
		for index := range 48 {
			if found[fmt.Sprintf("%03d", index)] != 1 {
				t.Fatal("record overwritten or duplicated")
			}
		}
	}
}

func TestArchiveCollisionFailsClosedWithoutOverwrite(t *testing.T) {
	options := defaults(fileOptions(t, false)).Outputs[0]
	writer, err := openRotatingFile(options)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte(strings.Repeat("x", 1023) + "\n")
	if _, err := writer.Write(data); err != nil {
		t.Fatal(err)
	}
	collision := filepath.Join(options.Directory, "archive-00000000000000000001.log")
	if err := os.WriteFile(collision, []byte("sentinel\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("next\n")); !errors.Is(err, unix.EEXIST) {
		t.Fatal("archive collision was not rejected", err)
	}
	if got := archiveBytes(t, collision); string(got) != "sentinel\n" {
		t.Fatal("existing backup overwritten")
	}
	if got := archiveBytes(t, filepath.Join(options.Directory, "current.log")); !bytes.Equal(got, data) {
		t.Fatal("current data lost")
	}
	for range 2 {
		if _, err := writer.Write([]byte("retry\n")); !errors.Is(err, unix.EEXIST) {
			t.Fatal("failed sink silently resumed")
		}
		if err := writer.Close(); !errors.Is(err, unix.EEXIST) {
			t.Fatal("cleanup erased earlier failure")
		}
	}
}

func TestExclusiveDirectoryAndPartialInitializationCleanup(t *testing.T) {
	options := fileOptions(t, false)
	first := bindFixture(t, options, nil, 2)
	selected, err := Select(options, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resourceAssembly(selected)
	if !errors.Is(err, unix.EWOULDBLOCK) || second == nil {
		t.Fatal("second owner acquired locked file", err)
	}
	if err := second.Close(context.Background()); err != nil {
		t.Fatal("partial failed owner leaked resources", err)
	}
	if result := logResult(t, first.logger, "still-owned"); result.Err() != nil {
		t.Fatal("failed owner closed first owner's descriptor")
	}
	if err := first.assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	third, err := resourceAssembly(selected)
	if err != nil {
		t.Fatal("owner did not release directory lock", err)
	}
	if err := third.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	valid := fileOptions(t, false)
	valid.Outputs = append(valid.Outputs, OutputV1{Name: "missing", Kind: "file", Directory: filepath.Join(t.TempDir(), "absent")})
	broken, err := Select(valid, nil)
	if err != nil {
		t.Fatal(err)
	}
	assembly, err := resourceAssembly(broken)
	if err == nil || assembly == nil {
		t.Fatal("missing second output accepted")
	}
	for _, source := range assembly.Snapshot().Sources {
		if source.Pending || !source.Released {
			t.Fatal("partial initialization leaked")
		}
	}
	probe, err := openRotatingFile(defaults(valid).Outputs[0])
	if err != nil {
		t.Fatal("earlier sink lock retained after construction failure", err)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDirtyDirectoriesAreRejectedWithoutRepairOrDeletion(t *testing.T) {
	for _, mode := range []string{"unknown", "temporary", "truncated", "symlink", "hardlink", "permissions", "oversized", "sequence-zero"} {
		t.Run(mode, func(t *testing.T) {
			options := defaults(fileOptions(t, true)).Outputs[0]
			name := "current.log"
			data := []byte("complete\n")
			switch mode {
			case "unknown":
				name = "unrelated"
			case "temporary":
				name = "archive-00000000000000000001.log.gz.tmp"
			case "truncated":
				data = []byte("incomplete")
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(options.Directory, name)); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				target := filepath.Join(t.TempDir(), "outside")
				if err := os.WriteFile(target, data, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(target, filepath.Join(options.Directory, name)); err != nil {
					t.Fatal(err)
				}
			case "oversized":
				data = bytes.Repeat([]byte("x"), 1025)
			case "sequence-zero":
				name = "archive-00000000000000000000.log.gz"
			}
			if mode != "symlink" && mode != "hardlink" {
				permission := os.FileMode(0600)
				if mode == "permissions" {
					permission = 0644
				}
				if err := os.WriteFile(filepath.Join(options.Directory, name), data, permission); err != nil {
					t.Fatal(err)
				}
			}
			writer, err := openRotatingFile(options)
			if err == nil {
				writer.Close()
				t.Fatal("unsafe restart accepted")
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(options.Directory)
			if err != nil || len(entries) != 1 || entries[0].Name() != name {
				t.Fatal("initialization repaired or deleted evidence", err)
			}
		})
	}
}

type errorReader struct{ err error }

func (reader errorReader) Read([]byte) (int, error) { return 0, reader.err }
func TestCompressionErrorAndSpaceBounds(t *testing.T) {
	native := errors.New("compression-cause-canary")
	if err := compressGZIP(strings.NewReader("payload"), &shortSink{cause: native}, 1024); !errors.Is(err, native) {
		t.Fatal("compression write error swallowed")
	}
	if err := compressGZIP(errorReader{native}, io.Discard, 1024); !errors.Is(err, native) {
		t.Fatal("compression read error swallowed")
	}
	if err := compressGZIP(strings.NewReader(strings.Repeat("x", 1025)), io.Discard, 1024); !errors.Is(err, ErrLimit) {
		t.Fatal("compression input limit not enforced")
	}
	writer := &cappedWriter{writer: io.Discard, remaining: 1}
	if _, err := writer.Write([]byte("xx")); !errors.Is(err, ErrLimit) {
		t.Fatal("compression output limit bypass")
	}
}

func TestRealCompressionFailureChild(t *testing.T) {
	if os.Getenv("FATHOMRY_ZAP_COMPRESSION_CHILD") != "1" {
		return
	}
	options := fileOptions(t, true)
	options.MaxEntryBytes = 4096
	options.Outputs[0].MaxFileBytes = 4096
	fixture := bindFixture(t, options, nil, 4)
	fixture.allowCloseError = true
	var random [2400]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	message := base64.RawStdEncoding.EncodeToString(random[:])
	receipt, err := fixture.logger.Log(context.Background(), fault.Correlation{Call: "before"}, zapcore.InfoLevel, message)
	if result := resultOf(t, receipt, err); result.Err() != nil {
		t.Fatal(result.Err())
	}
	var original unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &original); err != nil {
		t.Fatal(err)
	}
	signal.Ignore(syscall.SIGXFSZ)
	limited := original
	limited.Cur = 256
	if err := unix.Setrlimit(unix.RLIMIT_FSIZE, &limited); err != nil {
		t.Fatal(err)
	}
	defer unix.Setrlimit(unix.RLIMIT_FSIZE, &original)
	receipt, err = fixture.logger.Log(context.Background(), fault.Correlation{Call: "rotate"}, zapcore.InfoLevel, message)
	result := resultOf(t, receipt, err)
	if !errors.Is(result.Err(), unix.EFBIG) || result.Outcome.Value.SinksCopy()[0].State != Failed {
		t.Fatal("native compression failure not visible")
	}
	for range 2 {
		if err := fixture.assembly.Close(context.Background()); !errors.Is(err, unix.EFBIG) {
			t.Fatal("cleanup erased native compression failure")
		}
	}
	raw := filepath.Join(options.Outputs[0].Directory, "archive-00000000000000000001.log")
	if records := readJSON(t, raw); len(records) != 1 || records[0]["msg"] != message {
		t.Fatal("compression failure lost original")
	}
	reopened, err := openRotatingFile(defaults(options).Outputs[0])
	if err == nil {
		reopened.Close()
		t.Fatal("unfinished compression falsely recovered")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("native compression failure retained safely")
}
func TestRealCompressionFailurePropagatesWithoutStderr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRealCompressionFailureChild$", "-test.v", "-test.timeout=15s")
	command.Env = append(os.Environ(), "FATHOMRY_ZAP_COMPRESSION_CHILD=1", "GORACE=atexit_sleep_ms=0")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil || ctx.Err() != nil || !bytes.Contains(output, []byte("native compression failure retained safely")) || stderr.Len() != 0 {
		t.Fatalf("native compression failure witness failed: %v\n%s", err, output)
	}
}
