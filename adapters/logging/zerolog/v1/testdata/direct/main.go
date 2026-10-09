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

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zerolog "github.com/frost-leo/fathomry/adapters/logging/zerolog/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("zerolog direct public consumer passed")
}

func ptr[T any](value T) *T { return &value }

func markerState(directory string, owned bool) error {
	info, err := os.Stat(filepath.Join(directory, ".fathomry.lock"))
	if !owned && errors.Is(err, os.ErrNotExist) {
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			path, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if path == directory || strings.HasPrefix(path, directory+string(filepath.Separator)) {
				return errors.New("closed physical owner retained a file or directory descriptor")
			}
		}
		return nil
	}
	if err != nil || !owned || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return errors.New("exclusive ownership marker state was not preserved")
	}
	return nil
}

func cleanup(close func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = close(ctx)
}

func wait(ctx context.Context, receipt *adapters.Receipt[zerolog.Result], setup error, operation string) error {
	if setup != nil {
		return setup
	}
	if receipt == nil {
		return errors.New("accepted operation has no receipt")
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	if err := snapshot.Err(); err != nil {
		return err
	}
	value, present := snapshot.ValueCopy()
	if !present || !value.HasData() || len(value.SinksCopy()) != 1 || value.Source().Name != "direct" {
		return errors.New("per-destination facts lost original source")
	}
	sink := value.SinksCopy()[0]
	if sink.Filtered || sink.Stopped || sink.WriteError != nil || sink.MaintenanceError != nil ||
		operation == "log" && (!sink.Attempted || !sink.Accepted || !sink.BytesKnown || sink.Written < 1) ||
		operation == "sync" && !sink.Synced || operation == "rotate" && !sink.Rotated {
		return errors.New("native output and maintenance facts were conflated")
	}
	return nil
}

func run(ctx context.Context) error {
	directory, err := os.MkdirTemp("", "fathomry-zerolog-direct-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	prepared, err := zerolog.Prepare(zerolog.Settings{Name: "direct", Version: 1, Caller: ptr(true),
		Sinks: []zerolog.Sink{{Name: "local", Kind: "file", File: &zerolog.File{Directory: directory}}}})
	if err != nil {
		return err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return err
	}
	operations, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer cleanup(operations.Close)
	inbox, err := adapters.NewInbox[zerolog.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, zerolog.Dependencies{Runtime: operations, Evidence: inbox})
	if owner != nil {
		defer cleanup(owner.Close)
	}
	if err != nil {
		return err
	}
	if err := markerState(directory, true); err != nil {
		return err
	}
	client := owner.Client()
	if enabled, err := client.Enabled(ctx, zerolog.Debug); err != nil || enabled {
		return errors.New("frozen advisory threshold did not preserve the info minimum")
	}
	data := []byte{0, 255}
	attrs := []slog.Attr{slog.Uint64("unsigned", math.MaxUint64), slog.Int64("signed", math.MinInt64),
		zerolog.Attribute("float32", logging.Float32(1.2)), slog.Any("null", nil), slog.String("text", "nul\x00text"),
		zerolog.Attribute("array", logging.Array(logging.Null(), logging.Binary(data), logging.Array(), logging.Group()))}
	_, _, callerLine, _ := runtime.Caller(0)
	receipt, err := client.Log(ctx, zerolog.Info, "direct", attrs...)
	if err := wait(ctx, receipt, err, "log"); err != nil {
		return err
	}
	view, err := client.With(ctx, slog.String("bound", "original"), zerolog.Attribute("binary", logging.Binary(data)))
	if err != nil {
		return err
	}
	defer cleanup(view.Close)
	data[0] = 9
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	receipt, err = view.LogEntry(ctx, zerolog.Entry{Time: stamp, Level: zerolog.Info, Message: "derived"})
	if err := wait(ctx, receipt, err, "log"); err != nil {
		return err
	}
	handler, err := client.Slog(ctx)
	if err != nil {
		return err
	}
	defer cleanup(handler.Close)
	logger := slog.New(handler).With("before", "root").WithGroup("group").With("inside", 7)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	logger.InfoContext(canceled, "slog", "leaf", true)
	if err := handler.Handle(canceled, slog.NewRecord(time.Time{}, slog.LevelInfo, "zero", 0)); err != nil {
		return fmt.Errorf("slog cancellation suppressed an owner-bounded attempt: %w", err)
	}
	var definition failure.Definition
	for _, candidate := range zerolog.Definitions() {
		if candidate.Code == zerolog.ErrWrite {
			definition = candidate
		}
	}
	original, err := failure.New(definition, failure.Location{Operation: "business"}, errors.New("private-native-canary"))
	if err != nil {
		return err
	}
	slog.New(handler).ErrorContext(canceled, "safe-error", "error", original)
	invalid := handler.WithAttrs([]slog.Attr{slog.String("duplicate", "first"), slog.String("duplicate", "second")})
	slog.New(invalid).Info("must-not-appear")
	status, err := handler.Status()
	if err != nil || status.Admitted != 3 || status.InvalidDerivations == 0 || status.LastError == nil {
		return errors.New("restricted slog invalid derivation was not observable independently")
	}
	if receipt, err := client.Log(canceled, zerolog.Info, "direct-canceled"); receipt != nil || err == nil {
		return errors.New("direct logging borrowed the slog cancellation contract")
	}
	for _, severity := range []zerolog.Level{zerolog.Fatal, zerolog.Panic} {
		receipt, err := client.Log(ctx, severity, string(severity))
		if err := wait(ctx, receipt, err, "log"); err != nil {
			return err
		}
	}
	if err := handler.Close(ctx); err != nil {
		return err
	}
	if err := view.Close(ctx); err != nil {
		return err
	}
	receipt, err = client.Sync(ctx)
	if err := wait(ctx, receipt, err, "sync"); err != nil {
		return err
	}
	receipt, err = client.Rotate(ctx)
	if err := wait(ctx, receipt, err, "rotate"); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(err, errors.New("source cleanup remained incomplete"))
	}
	if err := operations.Close(ctx); err != nil {
		return err
	}
	if err := markerState(directory, false); err != nil {
		return err
	}
	if err := inbox.Seal(); err != nil {
		return err
	}
	seen := map[uint64]bool{}
	finite, lifecycle := 0, 0
	for {
		delivery, err := inbox.NextReleased(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			return err
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil || snapshot.Err() != nil || seen[snapshot.Info().Sequence] || !snapshot.Info().Released {
			return errors.New("independent evidence lost identity, outcome or release facts")
		}
		seen[snapshot.Info().Sequence] = true
		value, present := snapshot.ValueCopy()
		if !present || value.Source().Name != "direct" {
			return errors.New("independent evidence lost original source")
		}
		if value.HasData() {
			finite++
		} else {
			lifecycle++
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	if finite != 9 || lifecycle != 4 {
		return fmt.Errorf("independent evidence count: finite=%d lifecycle=%d", finite, lifecycle)
	}
	if status, err := inbox.Inspect(); err != nil || status.Outstanding != 0 || status.Bytes != 0 {
		return errors.New("required evidence custody remained after cleanup")
	}
	return verifyFiles(directory, callerLine+1, stamp)
}

func verifyFiles(directory string, callerLine int, stamp time.Time) error {
	archives, err := filepath.Glob(filepath.Join(directory, "log-*"))
	if err != nil || len(archives) != 1 {
		return errors.New("explicit rotation did not create exactly one archive")
	}
	active, err := os.Stat(filepath.Join(directory, "current.jsonl"))
	if err != nil || active.Size() != 0 {
		return errors.New("rotation did not establish an empty active file")
	}
	file, err := os.Open(archives[0])
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	records := make(map[string]map[string]any)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "private-native-canary") {
			return errors.New("private error cause entered local output")
		}
		decoder := json.NewDecoder(strings.NewReader(scanner.Text()))
		decoder.UseNumber()
		var record map[string]any
		if err := decoder.Decode(&record); err != nil {
			return err
		}
		message, _ := record["message"].(string)
		if _, duplicate := records[message]; duplicate {
			return errors.New("local event was duplicated")
		}
		records[message] = record
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(records) != 7 || records["fatal"]["level"] != "fatal" || records["panic"]["level"] != "panic" {
		return errors.New("severity-only events or refused-record behavior changed")
	}
	direct := records["direct"]["attributes"].(map[string]any)
	caller, _ := records["direct"]["caller"].(map[string]any)
	array, _ := direct["array"].([]any)
	if direct["unsigned"] != json.Number("18446744073709551615") || direct["signed"] != json.Number("-9223372036854775808") || direct["float32"] != json.Number("1.2") ||
		direct["text"] != "nul\x00text" || len(array) != 4 || array[0] != nil || array[1] != "AP8=" || caller["line"] != json.Number(strconv.Itoa(callerLine)) || !strings.HasSuffix(caller["file"].(string), "/main.go") {
		return errors.New("closed/native values or original business caller changed")
	}
	if value, present := direct["null"]; !present || value != nil {
		return errors.New("explicit null disappeared")
	}
	if empty, ok := array[2].([]any); !ok || len(empty) != 0 {
		return errors.New("empty array became null")
	}
	if empty, ok := array[3].(map[string]any); !ok || len(empty) != 0 {
		return errors.New("empty map became null")
	}
	derived := records["derived"]["attributes"].(map[string]any)
	if derived["bound"] != "original" || derived["binary"] != "AP8=" || records["derived"]["time"] != stamp.Format(time.RFC3339Nano) {
		return errors.New("frozen With data or explicit event time changed")
	}
	slogged := records["slog"]["attributes"].(map[string]any)
	group, _ := slogged["group"].(map[string]any)
	if slogged["before"] != "root" || group["inside"] != json.Number("7") || group["leaf"] != true {
		return errors.New("slog WithAttrs/WithGroup chronology changed")
	}
	if _, present := records["zero"]["time"]; present {
		return errors.New("zero slog time became a fabricated current timestamp")
	}
	if _, present := records["zero"]["caller"]; present {
		return errors.New("zero slog PC became a wrapper caller")
	}
	return nil
}
