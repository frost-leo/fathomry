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
	"os"
	"path/filepath"
	"strings"
	"testing"

	mail "github.com/frost-leo/fathomry/internal/notification/mail/v0"
	"golang.org/x/net/html"
)

func TestWelcomeContentAndNoActiveResources(t *testing.T) {
	content, err := welcomeContent("sender@fixture.test", "recipient@fixture.test", "welcome@fixture.test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = mail.NewMessage(content); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(content.Text, "Welcome to Fathomry") || len(content.Attachments) != 1 || len(content.Inline) != 0 {
		t.Fatal("welcome content changed")
	}
	tokenizer := html.NewTokenizer(strings.NewReader(content.HTML))
	links := 0
	for {
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			break
		}
		if kind != html.StartTagToken && kind != html.SelfClosingTagToken {
			continue
		}
		token := tokenizer.Token()
		switch token.Data {
		case "script", "iframe", "object", "form", "img":
			t.Fatal("welcome fixture contains active or remote content")
		}
		for _, attr := range token.Attr {
			if strings.HasPrefix(attr.Key, "on") {
				t.Fatal("welcome fixture contains event handlers")
			}
			if attr.Key == "href" {
				if attr.Val != "https://github.com/frost-leo/fathomry" && attr.Val != "https://github.com/frost-leo/fathomry/blob/develop/docs/README.md" {
					t.Fatal("welcome fixture contains an unexpected link")
				}
				links++
			}
		}
	}
	if links != 2 {
		t.Fatal("welcome navigation missing")
	}
}

func TestCommandRequiresAnExplicitMode(t *testing.T) {
	for _, args := range [][]string{nil, {"-send"}, {"-send", "-preview", "unused", "-config", "unused"}, {"-preview", "unused", "-config", "unused"}, {"unexpected"}} {
		if run(context.Background(), args, &bytes.Buffer{}) == nil {
			t.Fatal("ambiguous or unsafe command accepted")
		}
	}
}
func TestPreviewNeverSendsAndNeverOverwrites(t *testing.T) {
	output := filepath.Join(t.TempDir(), "welcome.html")
	if err := run(context.Background(), []string{"-preview", output}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(output)
	if err != nil || !bytes.Contains(data, []byte("Welcome to")) {
		t.Fatal("preview missing")
	}
	if err = run(context.Background(), []string{"-preview", output}, &bytes.Buffer{}); err == nil {
		t.Fatal("existing output overwritten")
	}
}
func TestPrivateConfigurationAndDisabledSending(t *testing.T) {
	file := filepath.Join(t.TempDir(), "private.yaml")
	raw := []byte("fathomry_mail_gh63:\n  enabled: false\n  smtp_password: private-test-canary\n")
	if err := os.WriteFile(file, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"-send", "-config", file}, &bytes.Buffer{}); err == nil || strings.Contains(err.Error(), "private-test-canary") {
		t.Fatal("disabled sending or diagnostic privacy failed")
	}
	if err := os.Chmod(file, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSettings(file); err == nil {
		t.Fatal("publicly readable credential file accepted")
	}
}
