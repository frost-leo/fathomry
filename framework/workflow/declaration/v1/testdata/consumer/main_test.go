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
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
)

type shortWriter struct{}

func (shortWriter) Write([]byte) (int, error) { return 0, nil }

type failedWriter struct{}

func (failedWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type cancelingWriter struct{ stop context.CancelFunc }

func (writer cancelingWriter) Write(value []byte) (int, error) {
	writer.stop()
	return len(value), nil
}

func TestExport(t *testing.T) {
	var output bytes.Buffer
	if err := export(context.Background(), &output); err != nil {
		t.Fatal(err)
	}
	bundle, err := wf.Parse(context.Background(), output.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	report, err := wf.Check(context.Background(), bundle)
	if err != nil || !report.Passed || report.Workflows != 1 || report.Modes != 2 {
		t.Fatal(report, err)
	}
	contents, err := bundle.ContentsCopy()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := wf.ParseContents(context.Background(), contents)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := restored.BytesCopy()
	if err != nil || !bytes.Equal(raw, output.Bytes()) {
		t.Fatal("public content reconstruction changed the portable bundle", err)
	}
	if err := export(context.Background(), shortWriter{}); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal(err)
	}
	if err := export(context.Background(), failedWriter{}); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	output.Reset()
	if err := export(ctx, &output); !errors.Is(err, context.Canceled) || output.Len() != 0 {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	if err := export(ctx, cancelingWriter{stop: cancel}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation after writing was reported as success", err)
	}
}
