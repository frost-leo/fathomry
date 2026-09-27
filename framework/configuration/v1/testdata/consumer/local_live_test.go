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

package consumer

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	source "github.com/frost-leo/fathomry/adapters/configsource/v1"
	local "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	c "github.com/frost-leo/fathomry/framework/configuration/v1"
)

func TestLocalRawCaptureBoundsAndPresence(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "source")
	for _, encoding := range []string{"yaml", "json"} {
		settings := local.Settings{Name: "raw", Documents: []local.File{{Name: "slot", Path: path, Encoding: encoding}}}
		value, err := local.Select(settings)
		if err != nil {
			t.Fatal(err)
		}
		settings.Documents[0].Path = "mutated"
		for _, raw := range []string{"", "   \n", "{not valid json", "x: 1\nx: 2"} {
			replace(t, path, raw)
			batch, err := value.Capture(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			copied, presence, err := batch.RawCopy("slot")
			if err != nil || presence != source.Present || string(copied) != raw {
				t.Fatal("raw capture normalized or lost empty")
			}
			if len(copied) > 0 {
				copied[0] = '!'
				again, _, _ := batch.RawCopy("slot")
				if string(again) != raw {
					t.Fatal("raw alias")
				}
			}
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		batch, err := value.Capture(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if raw, presence, err := batch.RawCopy("slot"); err != nil || presence != source.Missing || len(raw) != 0 {
			t.Fatal("missing not explicit")
		}
		replace(t, path, string([]byte{0xff}))
		if batch, err := value.Capture(context.Background()); batch != nil || !errors.Is(err, local.ErrEncoding) {
			t.Fatal("invalid UTF-8 accepted", err)
		}
		replace(t, path, strings.Repeat("x", source.MaxDocumentBytes+1))
		if batch, err := value.Capture(context.Background()); batch != nil || !errors.Is(err, local.ErrLimit) {
			t.Fatal("document limit failed", err)
		}
	}
	settings := local.Settings{Name: "total"}
	for index := range 5 {
		settings.Documents = append(settings.Documents, local.File{Name: string(rune('a' + index)), Path: file(t, directory, string(rune('a'+index)), strings.Repeat("x", source.MaxDocumentBytes)), Encoding: "yaml"})
	}
	value, err := local.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := value.Capture(context.Background()); batch != nil || !errors.Is(err, local.ErrLimit) {
		t.Fatal("aggregate prefix escaped", err)
	}
	settings.Documents = settings.Documents[:4]
	value, err = local.Select(settings)
	if err != nil {
		t.Fatal(err)
	}
	if batch, err := value.Capture(context.Background()); err != nil || len(batch.Documents()) != 4 {
		t.Fatal("exact aggregate boundary rejected", err)
	}
}
func TestLocalWatchRecoveryDefaultsAndFrozenEnvironment(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "source")
	selection := selected(t, "live", path, time.Second)
	declared := schema()
	var calls atomic.Int32
	declared.Validate = func(value c.Settings[project]) error {
		calls.Add(1)
		if value.Project.Count < 0 {
			return errors.New("validator-secret-canary")
		}
		return nil
	}
	input := plan(selection, false)
	t.Setenv("GH96_FROZEN", "first")
	input.Variables = []c.Variable{{Name: "GH96_FROZEN", Field: "/project/text"}}
	live, err := c.Watch(context.Background(), declared, input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, live.Close) })
	failed := await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	if _, err := failed.Snapshot.ValueCopy(); !errors.Is(err, c.ErrValue) || !errors.Is(failed.Failure, c.ErrMissing) {
		t.Fatal("initial failure fabricated valid zero")
	}
	declared.Defaults.Project.Labels["default"] = "mutated"
	t.Setenv("GH96_FROZEN", "second")
	replace(t, path, "format: 1\nproject: {count: 8, labels: {temporary: present}}")
	first := await(t, live, func(state c.State[project]) bool { return state.Status == c.Ready })
	value, _ := first.Snapshot.ValueCopy()
	if value.Project.Text != "first" || value.Project.Labels["default"] != "kept" {
		t.Fatal("Watch admission not frozen")
	}
	firstDescription, _ := first.Snapshot.Description()
	wait, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	if _, err := live.Next(wait, first.Cursor); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unchanged source generated a state", err)
	}
	cancel()
	current, _ := live.Current()
	same, _ := current.Snapshot.Description()
	if same.Revision != firstDescription.Revision || calls.Load() != 1 {
		t.Fatal("no-op revalidation")
	}
	replace(t, path, "format: 1\nproject: {count: -1}")
	failed = await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	old, _ := failed.Snapshot.ValueCopy()
	if old.Project.Count != 8 {
		t.Fatal("failure mutated previous accepted value")
	}
	count := calls.Load()
	time.Sleep(1200 * time.Millisecond)
	if calls.Load() != count {
		t.Fatal("unchanged pure rejection retried")
	}
	replace(t, path, "format: 1\nproject: {count: 9}")
	next := await(t, live, func(state c.State[project]) bool { return state.Status == c.Ready })
	value, _ = next.Snapshot.ValueCopy()
	if value.Project.Count != 9 || len(value.Project.Labels) != 1 {
		t.Fatal("removed fields retained previous effective state")
	}
	fresh, err := c.Load(context.Background(), schema(), input)
	if err != nil {
		t.Fatal(err)
	}
	freshValue, _ := fresh.ValueCopy()
	if freshValue.Project.Text != "second" {
		t.Fatal("new Load did not recapture environment")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	failed = await(t, live, func(state c.State[project]) bool { return state.Status == c.Degraded })
	old, _ = failed.Snapshot.ValueCopy()
	if old.Project.Count != 9 {
		t.Fatal("required removal lost old snapshot")
	}
	input.Inputs[0].Documents[0].Optional = true
	optional, err := c.Watch(context.Background(), schema(), input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, optional.Close) })
	inherited := await(t, optional, func(state c.State[project]) bool { return state.Status == c.Ready })
	value, _ = inherited.Snapshot.ValueCopy()
	if value.Project.Count != 7 {
		t.Fatal("optional missing did not inherit original defaults")
	}
	if _, err := optional.Next(context.Background(), first.Cursor); !errors.Is(err, c.ErrCursor) {
		t.Fatal("foreign Framework cursor accepted", err)
	}
	closeOwner(t, live.Close)
	terminal, _ := live.Current()
	if terminal.Status != c.Closed {
		t.Fatal("unjoined close")
	}
	if _, err := live.Next(context.Background(), terminal.Cursor); !errors.Is(err, c.ErrClosed) {
		t.Fatal("terminal wait did not end", err)
	}
	old, _ = first.Snapshot.ValueCopy()
	if old.Project.Count != 8 {
		t.Fatal("closed owner invalidated old snapshot")
	}
}
func TestLiteralPathObserveAndPacing(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux stable-file/symlink profile")
	}
	directory := t.TempDir()
	target := file(t, directory, "target", "first")
	path := filepath.Join(directory, "link")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	value := selected(t, "literal", path, time.Second)
	observer, err := value.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	initial := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	replace(t, target, "second")
	updated := awaitRaw(t, observer, func(state source.State) bool { return state.Generation > initial.Generation })
	raw, _, _ := updated.Batch.RawCopy("document")
	if string(raw) != "second" {
		t.Fatal("external symlink target change missed")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	missing := awaitRaw(t, observer, func(state source.State) bool {
		if state.Batch == nil {
			return false
		}
		_, presence, _ := state.Batch.RawCopy("document")
		return presence == source.Missing
	})
	if missing.Status != source.Available {
		t.Fatal("positive missing treated as failed read")
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	failed := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Degraded })
	generation := failed.Generation
	attempts := 1
	deadline, cancel := context.WithTimeout(context.Background(), 2200*time.Millisecond)
	defer cancel()
	for {
		next, err := observer.Next(deadline, failed.Cursor)
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			break
		}
		failed = next
		if next.Status == source.Degraded {
			attempts++
		}
	}
	if attempts > 3 || attempts < 2 || failed.Generation != generation {
		t.Fatal("failed reads were unpaced or changed raw identity", attempts)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	replace(t, path, "recreated")
	recovered := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	raw, _, _ = recovered.Batch.RawCopy("document")
	if string(raw) != "recreated" {
		t.Fatal("failed read required another event")
	}
	other, err := value.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, other.Close) })
	if _, err := other.Next(context.Background(), recovered.Cursor); !errors.Is(err, source.ErrCursor) {
		t.Fatal("foreign source cursor accepted", err)
	}
	cancelWait, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := observer.Next(cancelWait, recovered.Cursor); !errors.Is(err, context.Canceled) {
		t.Fatal("wait cancellation lost", err)
	}
	still, _ := observer.Current()
	if still.Status != source.Available {
		t.Fatal("wait canceled owner")
	}
	closeOwner(t, observer.Close)
	terminal, _ := observer.Current()
	if terminal.Status != source.Closed {
		t.Fatal("source not joined")
	}
	if _, err := observer.Next(context.Background(), terminal.Cursor); !errors.Is(err, source.ErrClosed) {
		t.Fatal("closed source wait did not terminate", err)
	}
}

func TestParentReplacementAndInPlaceWriterCountercontrol(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux literal-path profile")
	}
	root := t.TempDir()
	parent := filepath.Join(root, "parent")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	path := file(t, parent, "settings", "format: 1\nproject: {count: 1}")
	selection := selected(t, "parent", path, time.Second)
	observer, err := selection.Observe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeOwner(t, observer.Close) })
	initial := awaitRaw(t, observer, func(state source.State) bool { return state.Status == source.Available })
	if err := os.Rename(parent, parent+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	replace(t, path, "format: 1\nproject: {count: 2}")
	updated := awaitRaw(t, observer, func(state source.State) bool {
		return state.Status == source.Available && state.Generation > initial.Generation
	})
	raw, _, _ := updated.Batch.RawCopy("document")
	if !strings.Contains(string(raw), "2") {
		t.Fatal("parent replacement retained old inode")
	}
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.WriteString("format: 1\nproject:\n  count: 3\n"); err != nil {
		t.Fatal(err)
	}
	partial, err := c.Load(context.Background(), schema(), plan(selection, false))
	if err != nil {
		t.Fatal(err)
	}
	first, _ := partial.ValueCopy()
	if first.Project.Count != 3 || first.Project.Text != "default" {
		t.Fatal("in-place intermediate witness changed")
	}
	if _, err := writer.WriteString("  text: completed\n"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	complete, err := c.Load(context.Background(), schema(), plan(selection, false))
	if err != nil {
		t.Fatal(err)
	}
	last, _ := complete.ValueCopy()
	if last.Project.Text != "completed" {
		t.Fatal("completed writer not read")
	}
	first, _ = partial.ValueCopy()
	if first.Project.Text != "default" {
		t.Fatal("snapshot was not frozen")
	}
}
