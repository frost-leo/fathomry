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
	"context"
	"debug/buildinfo"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/conformance"
	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type cancelWriter struct {
	native zapcore.WriteSyncer
	cancel func()
}

type cancelingSink struct {
	cancel context.CancelCauseFunc
	cause  error
}

func (sink cancelingSink) Write(ctx context.Context, _ zapcore.Entry, _ []zapcore.Field) error {
	sink.cancel(sink.cause)
	return ctx.Err()
}

func (sink cancelingSink) Sync(ctx context.Context) error {
	sink.cancel(sink.cause)
	return ctx.Err()
}

func TestExtensionCancellationRetainsOriginalCause(t *testing.T) {
	for _, operation := range []string{"write", "sync", "close"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			cause := errors.New("extension-cancellation-canary")
			fixture := bindFixture(t, OptionsV1{Name: "logs"}, cancelingSink{cancel, cause}, 2)
			fixture.allowCloseError = operation == "close"
			var observed error
			switch operation {
			case "write":
				receipt, err := fixture.logger.Log(ctx, fault.Correlation{Call: "canceled"}, zapcore.InfoLevel, "message")
				observed = resultOf(t, receipt, err).Err()
			case "sync":
				receipt, err := fixture.logger.Sync(ctx, fault.Correlation{Call: "canceled"})
				observed = resultOf(t, receipt, err).Err()
			case "close":
				observed = fixture.assembly.Close(ctx)
			}
			if !errors.Is(observed, context.Canceled) || !errors.Is(observed, cause) {
				t.Fatal("extension cancellation lost its original cause")
			}
		})
	}
}

func (writer *cancelWriter) Write(data []byte) (int, error) {
	count, err := writer.native.Write(data)
	writer.cancel()
	return count, err
}
func (writer *cancelWriter) Sync() error { return writer.native.Sync() }

func TestBrokenLoggingControlChild(t *testing.T) {
	mode := os.Getenv("FATHOMRY_ZAP_BROKEN_CONTROL")
	if mode == "" {
		return
	}
	cause := errors.New("private-control-canary")
	sink := &recordingSink{writeErr: cause}
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, sink, 2)
	result := logResult(t, fixture.logger, "failure")
	switch mode {
	case "delivery":
		result.Outcome.Value.sinks[0].State = Written
		if result.Outcome.Value.SinksCopy()[0].State != Failed {
			t.Error("zap contract: failed sink certified written")
		}
	case "history":
		result.Outcome.Primary = nil
		if !errors.Is(result.Err(), cause) {
			t.Error("zap contract: native failure history erased")
		}
	case "privacy":
		conformance.Private(t, cause, "private-control-canary")
	case "native":
		conformance.Facade(t, sdk.NewNop(), "Log")
	default:
		t.Fatal("unknown broken control")
	}
	t.Log("zap negative oracle returned")
}
func TestBrokenLoggingControlsFailTheirIntendedOracle(t *testing.T) {
	for mode, diagnostic := range map[string]string{"delivery": "failed sink certified written", "history": "native failure history erased",
		"privacy": "diagnostic projection disclosed", "native": "dynamic facade exposes"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBrokenLoggingControlChild$", "-test.v", "-test.timeout=15s")
			command.Env = append(os.Environ(), "FATHOMRY_ZAP_BROKEN_CONTROL="+mode, "GORACE=atexit_sleep_ms=0")
			output, err := command.CombinedOutput()
			var failure *exec.ExitError
			if !errors.As(err, &failure) || failure.ExitCode() != 1 || ctx.Err() != nil || !bytes.Contains(output, []byte(diagnostic)) ||
				!bytes.Contains(output, []byte("zap negative oracle returned")) || bytes.Contains(output, []byte("panic:")) || bytes.Contains(output, []byte("private-control-canary")) {
				t.Fatal("broken control failed for wrong reason")
			}
		})
	}
}

func TestActualZapConsumingExecutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "zap-consumer")
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./testdata/consumer")
	command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("consumer build failed: %v\n%s", err, output)
	}
	output, err := exec.CommandContext(ctx, binary).Output()
	if err != nil {
		t.Fatal("consumer execution failed")
	}
	var report struct {
		Go                         string
		Written, Context, Released bool
		Modules                    map[string]string
	}
	if json.Unmarshal(output, &report) != nil || report.Go != runtime.Version() || !report.Written || !report.Context || !report.Released {
		t.Fatal("consumer behavior not verified")
	}
	build, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	for path, version := range map[string]string{"go.uber.org/zap": "v1.28.0", "go.uber.org/multierr": "v1.10.0", "golang.org/x/sys": "v0.47.0"} {
		found := false
		for _, dependency := range build.Deps {
			if dependency.Path == path {
				found = dependency.Version == version && dependency.Sum != "" && dependency.Replace == nil && report.Modules[path] == version
			}
		}
		if !found {
			t.Fatal("unverified consuming SDK dependency")
		}
	}
	for _, dependency := range build.Deps {
		for _, unselected := range []string{"nacos", "pgx", "zerolog", "timberjack", "lumberjack"} {
			if strings.Contains(dependency.Path, unselected) {
				t.Fatal("unselected provider linked")
			}
		}
	}
}

func TestShutdownRetainsSyncFailure(t *testing.T) {
	cause := errors.New("private-sync-canary")
	sink := &recordingSink{syncErr: cause}
	fixture := bindFixture(t, OptionsV1{Name: "logs"}, sink, 2)
	fixture.allowCloseError = true
	if result := logResult(t, fixture.logger, "written"); result.Err() != nil {
		t.Fatal(result.Err())
	}
	for range 2 {
		if err := fixture.assembly.Close(context.Background()); !errors.Is(err, cause) {
			t.Fatal("cleanup history lost")
		}
	}
	if sink.syncs != 1 {
		t.Fatal("Close retried extension or manufactured later success")
	}
	if _, err := fixture.logger.Sync(context.Background(), fault.Correlation{Call: "closed"}); err == nil {
		t.Fatal("Sync after release admitted")
	}
}

func TestCancellationBetweenSinksPreservesEarlierEffect(t *testing.T) {
	options := fileOptions(t, false)
	options.Outputs = append(options.Outputs, OutputV1{Name: "second", Kind: "file", Directory: privateDirectory(t)})
	fixture := bindFixture(t, options, nil, 2)
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("between-sinks-canary")
	first := fixture.logger.owner.branches[0].writer
	first.native = &cancelWriter{native: first.native, cancel: func() { cancel(cause) }}
	receipt, err := fixture.logger.Log(ctx, fault.Correlation{Call: "partial-cancel"}, 0, "message")
	result := resultOf(t, receipt, err)
	sinks := result.Outcome.Value.SinksCopy()
	if sinks[0].State != Written || sinks[1].State != NotAttempted || !errors.Is(sinks[1].Err, cause) {
		t.Fatal("cancellation invented all-or-no sink effects")
	}
	if len(readJSON(t, filepath.Join(options.Outputs[0].Directory, "current.log"))) != 1 || len(readJSON(t, filepath.Join(options.Outputs[1].Directory, "current.log"))) != 0 {
		t.Fatal("per-sink effect oracle failed")
	}
}
