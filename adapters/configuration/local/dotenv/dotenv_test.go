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

package dotenv

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/frost-leo/fathomry/framework/configuration"
)

func TestLiteralInputsNeverExpandOrMutateEnvironment(t *testing.T) {
	t.Setenv("FATHOMRY_DOTENV_TEST", "ambient-secret")
	values, err := parse([]byte("EMPTY=\nexport NAME=plain # comment\nHASH=abc#def\nSINGLE='$FATHOMRY_DOTENV_TEST'\nDOUBLE=\"$FATHOMRY_DOTENV_TEST\\nnext\" # note\nCOMMAND=$(never-execute)\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"EMPTY": "", "NAME": "plain", "HASH": "abc#def", "SINGLE": "$FATHOMRY_DOTENV_TEST", "DOUBLE": "$FATHOMRY_DOTENV_TEST\nnext", "COMMAND": "$(never-execute)"}
	for name, expected := range want {
		value := values.Lookup(name)
		if !value.Present || value.Value != expected || value.Source != "dotenv" {
			t.Fatalf("literal %s changed", name)
		}
	}
	if os.Getenv("FATHOMRY_DOTENV_TEST") != "ambient-secret" {
		t.Fatal("process mutated")
	}
	if value := values.Lookup("ABSENT"); value.Present || value.Source != "" {
		t.Fatal("absence fabricated")
	}
	if strings.Contains(fmt.Sprintf("%+v %#v", values, values), "plain") {
		t.Fatal("format disclosed values")
	}
	if _, err := json.Marshal(values); err == nil {
		t.Fatal("values serialized")
	}
}

func TestProcessPrecedenceIncludesEmptyAndConcurrentReads(t *testing.T) {
	const name = "FATHOMRY_DOTENV_EMPTY"
	t.Setenv(name, "")
	values, err := parse([]byte(name + "=file"))
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for range 100 {
				value := values.LookupWithProcess(name)
				if !value.Present || value.Value != "" || value.Source != "process" {
					t.Error("empty process value lost")
				}
				if values.Lookup(name).Value != "file" {
					t.Error("file snapshot changed")
				}
			}
		})
	}
	workers.Wait()
}

func TestMalformedAndBoundedInputsAreAllOrNothing(t *testing.T) {
	for _, raw := range []string{"GOOD=yes\ninvalid", "KEY=x\nKEY=y", "1BAD=x", "A.B=x", "A: x", "export KEY", "KEY='unfinished", "KEY=\"bad\\q\"", "KEY=\"ok\" junk", "KEY=\x00", "KEY=\xff", "KEY='multi\nline'"} {
		values, err := parse([]byte(raw))
		if err == nil || values.entries != nil {
			t.Fatalf("invalid input accepted: %q", raw)
		}
	}
	if _, err := parse([]byte(strings.Repeat("#", MaxBytes))); err != nil {
		t.Fatal("exact byte limit refused")
	}
	if _, err := parse([]byte(strings.Repeat("#", MaxBytes+1))); !errors.Is(err, configuration.LimitExceeded) {
		t.Fatal("byte bound absent")
	}
	var lines strings.Builder
	for i := range MaxEntries {
		fmt.Fprintf(&lines, "KEY%d=x\n", i)
	}
	if _, err := parse([]byte(lines.String())); err != nil {
		t.Fatal("exact entry limit refused")
	}
	lines.WriteString("EXTRA=x\n")
	if _, err := parse([]byte(lines.String())); !errors.Is(err, configuration.LimitExceeded) {
		t.Fatal("entry bound absent")
	}
}

func TestReadFilePrivacyAbsenceAndCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private.env")
	if err := os.WriteFile(path, []byte("VALUE=private-test-value\n"), 0600); err != nil {
		t.Fatal(err)
	}
	values, err := ReadFile(context.Background(), path)
	if err != nil || values.Lookup("VALUE").Value != "private-test-value" {
		t.Fatal("read failed")
	}
	if err := os.WriteFile(path, []byte("VALUE=replaced"), 0600); err != nil {
		t.Fatal(err)
	}
	if values.Lookup("VALUE").Value != "private-test-value" {
		t.Fatal("snapshot not frozen")
	}
	for _, target := range []string{path + ".missing", filepath.Dir(path), "relative.env"} {
		values, err := ReadFile(context.Background(), target)
		if err == nil || values.entries != nil || strings.Contains(err.Error(), target) {
			t.Fatal("invalid file accepted or path disclosed")
		}
	}
	_, err = ReadFile(context.Background(), path+".missing")
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("absence category lost")
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("intentional")
	cancel(cause)
	if _, err := ReadFile(ctx, path); !errors.Is(err, context.Canceled) || !errors.Is(err, cause) {
		t.Fatal("cancellation lost")
	}
	if _, err := ReadFile(nil, path); !errors.Is(err, configuration.InvalidInput) {
		t.Fatal("nil context accepted")
	}
}

func FuzzLiteralDotenv(f *testing.F) {
	for _, seed := range []string{"KEY=value\n", "KEY=\"escaped\\ntext\"", "KEY='$HOME'", "KEY=x\nKEY=y"} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		values, err := parse(raw)
		if err != nil {
			if values.entries != nil {
				t.Fatal("partial parse escaped")
			}
			return
		}
		if len(raw) > MaxBytes || len(values.entries) > MaxEntries {
			t.Fatal("limits exceeded")
		}
		for key := range values.entries {
			if !validName(key) || !values.Lookup(key).Present {
				t.Fatal("invalid snapshot")
			}
		}
	})
}
