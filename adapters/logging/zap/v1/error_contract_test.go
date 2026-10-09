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

package zap_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"go.uber.org/zap/zapcore"
)

func TestErrorEvidenceKeepsOriginalFailureAfterSuccessfulSync(t *testing.T) {
	cause := errors.New("PRIVATE_WRITE_CAUSE")
	sink := &recorder{err: cause}
	owner, deps := loggerOwner(t, zap.Settings{Name: "history", Version: 1, Structured: true}, sink)
	value, err := logResult(t, deps, owner.Client(), zapcore.ErrorLevel, "message")
	if !errors.Is(err, cause) || !errors.Is(err, zap.ErrWrite) {
		t.Fatal("lost cause or class", err)
	}
	original := value.SinksCopy()[0]
	if !errors.Is(original.Err, cause) || strings.Contains(fmt.Sprintf("%+v", original.Err), "PRIVATE_WRITE_CAUSE") {
		t.Fatal("unsafe cause presentation")
	}
	sink.mu.Lock()
	sink.err = nil
	sink.mu.Unlock()
	receipt, submit := owner.Client().Sync(context.Background())
	success, syncErr := result(t, deps, receipt, submit)
	if syncErr != nil || success.SinksCopy()[0].State != zap.Synced || !errors.Is(original.Err, cause) {
		t.Fatal("Sync rewrote prior outcome")
	}
	copy := value.SinksCopy()
	copy[0].State = zap.Synced
	if value.SinksCopy()[0].State != zap.Failed {
		t.Fatal("mutable returned result")
	}
}

func TestFullRequiredEvidenceRefusalVisibleThroughSlog(t *testing.T) {
	sink := &recorder{}
	owner, deps := loggerOwner(t, zap.Settings{Name: "evidence", Version: 1, Structured: true}, sink)
	handler, err := owner.Client().Slog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	options, _ := deps.Evidence.Options()
	admitted := 0
	for index := 0; index < options.Capacity+1; index++ {
		receipt, err := owner.Client().Log(context.Background(), zapcore.InfoLevel, "fill")
		if receipt == nil {
			if !errors.Is(err, adapters.ErrEvidence) {
				t.Fatal("unexpected refusal", err)
			}
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(context.Background())
		if err != nil || snapshot.Err() != nil {
			t.Fatal(err, snapshot.Err())
		}
		admitted++
	}
	if admitted == 0 || admitted >= options.Capacity {
		t.Fatal("source/family evidence not reserved", admitted, options.Capacity)
	}
	before := len(sink.snapshot())
	slog.New(handler).Info("hidden refusal")
	after, _ := handler.Status()
	if after.Refused != 1 || after.Admitted != 0 || !errors.Is(after.LastError, adapters.ErrEvidence) || len(sink.snapshot()) != before {
		t.Fatal("hidden refusal was silent or wrote")
	}
	if err := handler.Close(context.Background()); err != nil {
		t.Fatal("cleanup needed new evidence admission", err)
	}
	if !handler.ShutdownComplete() {
		t.Fatal("gateway cleanup remained pending")
	}
}
