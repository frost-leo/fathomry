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
	"syscall"
	"time"

	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("zap direct public consumer passed")
}

func ptr[T any](value T) *T { return &value }

func lockState(directory string, available bool) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == nil {
		if release := syscall.Flock(int(file.Fd()), syscall.LOCK_UN); release != nil {
			return release
		}
		if !available {
			return errors.New("live physical file owner did not hold its directory lock")
		}
		return nil
	}
	if available || !errors.Is(err, syscall.EWOULDBLOCK) {
		return fmt.Errorf("physical lock release/state: %w", err)
	}
	return nil
}

func cleanup(close func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = close(ctx)
}

func wait(ctx context.Context, receipt *adapters.Receipt[zap.Result], err error, state zap.SinkState) error {
	if err != nil {
		return err
	}
	if receipt == nil {
		return errors.New("accepted logging operation has no receipt")
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	if err := snapshot.Err(); err != nil {
		return err
	}
	value, present := snapshot.ValueCopy()
	if !present || !value.HasData() || len(value.SinksCopy()) != 1 || value.SinksCopy()[0].State != state || value.Source().Name != "direct" {
		return errors.New("per-destination logging facts were incomplete")
	}
	return nil
}

func run(ctx context.Context) error {
	directory, err := os.MkdirTemp("", "fathomry-zap-direct-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	prepared, err := zap.Prepare(zap.Settings{Name: "direct", Version: 1, Caller: ptr(true), Outputs: []zap.Output{{Name: "local", Kind: "file", Directory: directory}}})
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
	inbox, err := adapters.NewInbox[zap.Result](policy.Evidence)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, zap.Dependencies{Runtime: operations, Evidence: inbox})
	if owner != nil {
		defer cleanup(owner.Close)
	}
	if err != nil {
		return err
	}
	if err := lockState(directory, false); err != nil {
		return err
	}
	client := owner.Client()
	if enabled, err := client.Enabled(ctx, zapcore.DebugLevel); err != nil || enabled {
		return errors.New("advisory threshold did not preserve the info minimum")
	}
	data := []byte{0, 255}
	fields := []zapcore.Field{sdk.Uint64("unsigned", math.MaxUint64), sdk.Int8("signed", -1), sdk.Float32("float32", 1.2),
		sdk.Stringp("null", nil), sdk.Error(nil), sdk.ByteString("text", []byte("nul\x00text")),
		zap.Field("array", logging.Array(logging.Null(), logging.Binary(data)))}
	_, _, line, _ := runtime.Caller(0)
	receipt, err := client.Log(ctx, zapcore.InfoLevel, "direct", fields...)
	if err := wait(ctx, receipt, err, zap.Written); err != nil {
		return err
	}
	data[0] = 9
	view, err := client.With(ctx, sdk.String("bound", "original"))
	if err != nil {
		return err
	}
	defer cleanup(view.Close)
	view, err = view.Named("business")
	if err != nil {
		return err
	}
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC)
	receipt, err = view.LogEntry(ctx, zap.Entry{Time: stamp, Level: zapcore.InfoLevel, Message: "derived"})
	if err := wait(ctx, receipt, err, zap.Written); err != nil {
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
	for _, candidate := range zap.Definitions() {
		if candidate.Code == zap.ErrWrite {
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
		return errors.New("restricted slog refused derivation was not observable independently")
	}
	if receipt, err := client.Log(canceled, zapcore.InfoLevel, "direct-canceled"); receipt != nil || err == nil {
		return errors.New("direct logging borrowed slog cancellation semantics")
	}
	if err := handler.Close(ctx); err != nil {
		return err
	}
	if err := view.Close(ctx); err != nil {
		return err
	}
	receipt, err = client.Sync(ctx)
	if err := wait(ctx, receipt, err, zap.Synced); err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil || !owner.ShutdownComplete() {
		return errors.Join(err, errors.New("physical/logical source cleanup remained incomplete"))
	}
	if err := operations.Close(ctx); err != nil {
		return err
	}
	if err := lockState(directory, true); err != nil {
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
			return errors.New("independent evidence lost failure/release/identity facts")
		}
		seen[snapshot.Info().Sequence] = true
		value, present := snapshot.ValueCopy()
		if !present || value.Source().Name != "direct" {
			return errors.New("independent evidence lost original source identity")
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
	if finite != 6 || lifecycle != 4 {
		return fmt.Errorf("independent evidence count changed: finite=%d lifecycle=%d", finite, lifecycle)
	}
	if status, err := inbox.Inspect(); err != nil || status.Outstanding != 0 || status.Bytes != 0 {
		return errors.New("required evidence custody remained after cleanup")
	}
	return verifyFile(directory, line+1, stamp)
}

func verifyFile(directory string, callerLine int, stamp time.Time) error {
	file, err := os.Open(filepath.Join(directory, "current.log"))
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
		message, _ := record["msg"].(string)
		if _, duplicate := records[message]; duplicate {
			return errors.New("local event was duplicated")
		}
		records[message] = record
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(records) != 5 {
		return errors.New("invalid or canceled input emitted a misleading partial record")
	}
	direct := records["direct"]
	caller, _ := direct["caller"].(string)
	array, _ := direct["array"].([]any)
	if direct["unsigned"] != json.Number("18446744073709551615") || direct["signed"] != json.Number("-1") || direct["float32"] != json.Number("1.2") ||
		direct["text"] != "nul\x00text" || len(array) != 2 || array[0] != nil || array[1] != "AP8=" || !strings.HasSuffix(caller, "main.go:"+strconv.Itoa(callerLine)) {
		return errors.New("local native tags, binary copy or original business caller changed")
	}
	if value, present := direct["null"]; !present || value != nil {
		return errors.New("native nil helper disappeared")
	}
	derived := records["derived"]
	if derived["bound"] != "original" || derived["logger"] != "business" || derived["ts"] != stamp.Format(time.RFC3339Nano) {
		return errors.New("frozen With/Named metadata changed")
	}
	group, _ := records["slog"]["group"].(map[string]any)
	if records["slog"]["before"] != "root" || group["inside"] != json.Number("7") || group["leaf"] != true {
		return errors.New("slog WithAttrs/WithGroup chronology changed")
	}
	if _, timestamp := records["zero"]["ts"]; timestamp {
		return errors.New("zero slog timestamp was replaced with current time")
	}
	if _, caller := records["zero"]["caller"]; caller {
		return errors.New("zero slog PC was replaced with a wrapper caller")
	}
	return nil
}
