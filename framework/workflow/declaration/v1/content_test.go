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

package declaration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestContentsRoundTripAndOwnership(t *testing.T) {
	for _, name := range []string{"health", "orders", "branching", "stage-gate"} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile("testdata/" + name + ".bundle.json")
			if err != nil {
				t.Fatal(err)
			}
			original, err := Parse(context.Background(), raw)
			if err != nil {
				t.Fatal(err)
			}
			contents, err := original.ContentsCopy()
			if err != nil {
				t.Fatal(err)
			}
			for index, content := range contents {
				if fingerprint(content.Entry.Profile, content.Bytes) != content.Entry.SHA256 {
					t.Fatal("copied content does not match its entry")
				}
				if index > 0 && compareIdentity(contents[index-1].Entry.Identity, content.Entry.Identity) >= 0 {
					t.Fatal("content identity order changed")
				}
				if fmt.Sprintf("%+v", content) != "workflow.Content" || content.LogValue().String() != "workflow.Content" {
					t.Fatal("ordinary diagnostics exposed content")
				}
			}
			slices.Reverse(contents)
			restored, err := ParseContents(context.Background(), contents)
			if err != nil {
				t.Fatal(err)
			}
			want, _ := original.BytesCopy()
			got, _ := restored.BytesCopy()
			if !bytes.Equal(got, want) {
				t.Fatal("content reconstruction changed the existing bundle protocol")
			}
			report, err := Check(context.Background(), original, restored)
			if err != nil || !report.Passed {
				t.Fatal("restored content did not compare equal", err)
			}
			for index := range contents {
				contents[index].Bytes[0] = '!'
				contents[index].Entry.Identity.Code = "changed"
			}
			again, _ := restored.BytesCopy()
			untouched, _ := original.BytesCopy()
			if !bytes.Equal(again, want) || !bytes.Equal(untouched, want) {
				t.Fatal("input or copied bytes aliased immutable content")
			}
			copied, _ := restored.ContentsCopy()
			if _, err := ParseContents(context.Background(), copied); err != nil {
				t.Fatal("copied content retained earlier mutated bytes", err)
			}
			var workers sync.WaitGroup
			for range 4 {
				workers.Go(func() {
					content, err := restored.ContentsCopy()
					if err != nil || len(content) == 0 {
						t.Error("concurrent copy failed", err)
						return
					}
					content[0].Bytes[0] = '!'
				})
			}
			workers.Wait()
		})
	}
}

func TestContentsSelectedParentAndLabels(t *testing.T) {
	original := mustCompile(t, fixtureDeclaration(t))
	contents, _ := original.ContentsCopy()
	parent := contents[0]
	mode := contents[1]
	selected, err := ParseContents(context.Background(), []Content{mode, parent})
	if err != nil {
		t.Fatal(err)
	}
	report, err := Check(context.Background(), selected)
	if err != nil || report.Workflows != 1 || report.Modes != 1 {
		t.Fatal("selected reconstruction loaded unrelated modes", err)
	}
	mode.Entry.Identity.Version = "another"
	if _, err := ParseContents(context.Background(), []Content{parent, mode}); !errors.Is(err, ErrInvalid) {
		t.Fatal("mode admitted under a different parent")
	}
	parent.Entry.Identity.Version = "another"
	relabeled, err := ParseContents(context.Background(), []Content{parent, mode})
	if err != nil {
		t.Fatal("opaque labels were incorrectly included in content digest", err)
	}
	relabeledContents, _ := relabeled.ContentsCopy()
	if relabeledContents[0].Entry.SHA256 != parent.Entry.SHA256 || relabeledContents[1].Entry.SHA256 != mode.Entry.SHA256 {
		t.Fatal("same-content labels lost independent identity")
	}
	input := fixtureDeclaration(t)
	control := input.Workflows[0].Spec.Controls["attempt_timeout"]
	control.Maximum = "1100"
	input.Workflows[0].Spec.Controls["attempt_timeout"] = control
	input.Workflows[0].Modes = nil
	differentParent := mustCompile(t, input)
	narrow, _ := differentParent.ContentsCopy()
	if _, err := ParseContents(context.Background(), []Content{narrow[0], contents[1]}); !errors.Is(err, ErrInvalid) {
		t.Fatal("mode was not validated against its selected parent's controls", err)
	}
}

func TestContentsRejectUntrustedStorage(t *testing.T) {
	bundle := mustCompile(t, fixtureDeclaration(t))
	cases := []struct {
		name string
		code error
		edit func([]Content) []Content
	}{
		{"empty", ErrHandle, func([]Content) []Content { return nil }},
		{"duplicate", ErrInvalid, func(value []Content) []Content { return append(value, value[0]) }},
		{"missing_parent", ErrInvalid, func(value []Content) []Content { return value[1:] }},
		{"half_identity", ErrInvalid, func(value []Content) []Content { value[0].Entry.Identity.Revision = "1"; return value }},
		{"unknown_profile", ErrUnsupported, func(value []Content) []Content { value[0].Entry.Profile = "future"; return value }},
		{"unknown_format", ErrUnsupported, func(value []Content) []Content {
			value[0].Bytes = bytes.ReplaceAll(value[0].Bytes, []byte(SpecFormat), []byte("fathomry.workflow-spec/v2"))
			return value
		}},
		{"invalid_namespace", ErrInvalid, func(value []Content) []Content { value[0].Entry.Identity.Namespace = "private\ncanary"; return value }},
		{"large_identity", ErrLimit, func(value []Content) []Content { value[0].Entry.Identity.Code = strings.Repeat("x", 129); return value }},
		{"broken_json", ErrInvalid, func(value []Content) []Content { value[0].Bytes = []byte("{"); return value }},
		{"unknown_field", ErrInvalid, func(value []Content) []Content {
			value[0].Bytes = append([]byte("{\"extra\":1,"), value[0].Bytes[1:]...)
			return value
		}},
		{"duplicate_field", ErrInvalid, func(value []Content) []Content {
			value[0].Bytes = append([]byte("{\"format\":\""+SpecFormat+"\","), value[0].Bytes[1:]...)
			return value
		}},
		{"missing_mode_value", ErrInvalid, func(value []Content) []Content {
			value[1].Bytes = []byte("{\"format\":\"" + ModeFormat + "\",\"values\":{}}")
			return value
		}},
		{"wrong_digest", ErrIntegrity, func(value []Content) []Content { value[0].Entry.SHA256 = strings.Repeat("0", 64); return value }},
		{"missing_digest", ErrIntegrity, func(value []Content) []Content { value[0].Entry.SHA256 = ""; return value }},
		{"spec_whitespace", ErrIntegrity, func(value []Content) []Content { value[0].Bytes = append([]byte(" "), value[0].Bytes...); return value }},
		{"mode_whitespace", ErrIntegrity, func(value []Content) []Content { value[1].Bytes = append(value[1].Bytes, '\n'); return value }},
		{"mode_member_order", ErrIntegrity, func(value []Content) []Content {
			value[1].Bytes = []byte("{\"values\":{\"attempt_timeout\":\"1500\"},\"format\":\"" + ModeFormat + "\"}")
			return value
		}},
		{"large_spec", ErrLimit, func(value []Content) []Content {
			value[0].Bytes = bytes.Repeat([]byte(" "), MaxSpecBytes+1)
			return value
		}},
		{"large_mode", ErrLimit, func(value []Content) []Content {
			value[1].Bytes = bytes.Repeat([]byte(" "), MaxModeBytes+1)
			return value
		}},
		{"entry_count", ErrLimit, func(value []Content) []Content { return make([]Content, MaxWorkflows+MaxModes+1) }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			content, _ := bundle.ContentsCopy()
			got, err := ParseContents(context.Background(), test.edit(content))
			if got != nil || !errors.Is(err, test.code) {
				detail, _ := Diagnose(err)
				t.Fatalf("untrusted content returned bundle=%t error=%v detail=%+v", got != nil, err, detail)
			}
			if strings.Contains(fmt.Sprint(err), "private") {
				t.Fatal("error exposed rejected identity")
			}
		})
	}
	for _, handle := range []*Bundle{nil, new(Bundle)} {
		if value, err := handle.ContentsCopy(); value != nil || !errors.Is(err, ErrHandle) {
			t.Fatal("invalid bundle exposed content")
		}
	}
	content, _ := bundle.ContentsCopy()
	if value, err := ParseContents(nil, content); value != nil || !errors.Is(err, ErrHandle) {
		t.Fatal("nil context admitted")
	}
}

func TestContentsCancellationAndAggregateLimits(t *testing.T) {
	bundle := mustCompile(t, fixtureDeclaration(t))
	content, _ := bundle.ContentsCopy()
	cause := errors.New("content-cancellation-canary")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if got, err := ParseContents(ctx, content); got != nil || !errors.Is(err, ErrCanceled) || !errors.Is(err, cause) {
		t.Fatal("canceled reconstruction lost cause", err)
	}
	for _, steps := range []int{2, 10, 100, 1000} {
		ctx, stop := cancelAfter(steps)
		got, err := ParseContents(ctx, content)
		stop()
		if got != nil || !errors.Is(err, ErrCanceled) {
			t.Fatal("cancellation returned partial content", steps, err)
		}
	}
	for _, count := range []int{MaxWorkflows + 1, 5} {
		entries := make([]Content, count)
		for index := range entries {
			entries[index] = content[0]
			entries[index].Entry.Identity.Version = fmt.Sprint(index)
			if count == 5 {
				entries[index].Bytes = bytes.Repeat([]byte(" "), MaxSpecBytes)
			}
		}
		if got, err := ParseContents(context.Background(), entries); got != nil || !errors.Is(err, ErrLimit) {
			t.Fatal("aggregate document/workflow bound was not applied before decoding", err)
		}
	}
	modes := []Content{content[0]}
	for index := range MaxModesPerWorkflow + 1 {
		mode := content[1]
		mode.Entry.Identity.Mode = fmt.Sprintf("mode_%d", index)
		modes = append(modes, mode)
	}
	if got, err := ParseContents(context.Background(), modes); got != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("per-parent mode bound was lost", err)
	}
}

func FuzzParseContents(f *testing.F) {
	bundle := mustCompile(f, fixtureDeclaration(f))
	content, _ := bundle.ContentsCopy()
	f.Add(content[0].Bytes, content[1].Bytes)
	f.Add([]byte("{}"), []byte("{}"))
	f.Fuzz(func(t *testing.T, spec, mode []byte) {
		if len(spec) > 64<<10 || len(mode) > 64<<10 {
			t.Skip()
		}
		input := []Content{
			{Entry: content[0].Entry, Bytes: spec},
			{Entry: content[1].Entry, Bytes: mode},
		}
		input[0].Entry.SHA256 = fingerprint(SpecProfile, spec)
		input[1].Entry.SHA256 = fingerprint(ModeProfile, mode)
		bundle, err := ParseContents(context.Background(), input)
		if err != nil {
			if bundle != nil {
				t.Fatal("partial bundle escaped")
			}
			return
		}
		copied, err := bundle.ContentsCopy()
		if err != nil || !bytes.Equal(copied[0].Bytes, spec) || !bytes.Equal(copied[1].Bytes, mode) {
			t.Fatal("admitted content was not canonical or detached", err)
		}
		if _, err := ParseContents(context.Background(), copied); err != nil {
			t.Fatal("accepted content failed reconstruction", err)
		}
	})
}
