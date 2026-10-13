/*
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

package chromedp

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/chromedp/cdproto/page"
)

func TestLivePDFStreamEvidenceAndAutomaticClose(t *testing.T) {
	path := os.Getenv("FATHOMRY_CHROME_EXECUTABLE")
	if os.Getenv("FATHOMRY_CHROMEDP_INTEGRATION") != "1" || path == "" {
		t.Skip("explicit browser fixture not selected")
	}
	p, err := Prepare(Settings{Name: "stream", ExecPath: path, TempDir: t.TempDir(), NewWindow: true, MaxSessions: new(1), MaxStreams: new(1), Flags: map[string]string{"headless": "", "no-first-run": ""}}, NativeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	d := mechanisms(t, p)
	owner, err := p.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(bounded(t)) })
	retained, _, err := owner.Client().Retain(bounded(t), ContextOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retained.Page().Navigate(bounded(t), "data:text/html,<h1>public PDF stream</h1>"); err != nil {
		t.Fatal(err)
	}
	drain(t, d, 1)
	stream, _, err := retained.Page().PDFStream(bounded(t), page.PrintToPDFParams{MarginTop: new(0.0)})
	if err != nil {
		t.Fatal(err)
	}
	var data []byte
	complete := false
	for range 256 {
		chunk, receipt, err := stream.Read(bounded(t), 1024)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := receipt.WaitReleased(bounded(t))
		if err != nil {
			t.Fatal(err)
		}
		evidence, present := snapshot.ValueCopy()
		if !present || !bytes.Equal(evidence.PayloadCopy(), chunk.DataCopy()) || evidence.Observation().EOF != chunk.EOF() {
			t.Fatal("chunk evidence")
		}
		data = append(data, chunk.DataCopy()...)
		drain(t, d, 1)
		if chunk.EOF() {
			complete = true
			break
		}
	}
	if !complete || !bytes.HasPrefix(data, []byte("%PDF-")) {
		t.Fatal("PDF or positive EOF absent")
	}
	// The owner closes an ignored stream handle, without fresh work admission.
	if err := retained.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	if !stream.Closed() {
		t.Fatal("stream producer left owned after context close")
	}
	if _, _, err := stream.Read(context.Background(), 1); err == nil {
		t.Fatal("closed stream revived")
	}
	if err := owner.Close(bounded(t)); err != nil {
		t.Fatal(err)
	}
	status, err := d.Evidence.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	drain(t, d, status.Outstanding)
}
