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

package nacos

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestRawReadSeamPreservesMissingEmptyAndRequestedNamespace(t *testing.T) {
	fixture := newFixture(t, false)
	input := fixture.options()
	input.Namespace = "test-namespace"
	input.Keys = append(input.Keys, KeyV1{DataID: "absent"})
	fixture.mu.Lock()
	fixture.values[key{"DEFAULT_GROUP", "settings.yaml"}] = ""
	fixture.mu.Unlock()
	client := openClient(t, input)
	documents, failedDocument, err := client.ReadRawAll(context.Background())
	if err != nil || failedDocument != -1 || len(documents) != 2 {
		t.Fatal("raw batch failed", err)
	}
	if documents[0].Missing() || len(documents[0].RawCopy()) != 0 || !documents[1].Missing() {
		t.Fatal("empty/missing collapsed")
	}
	for _, document := range documents {
		if document.Namespace() != input.Namespace || document.Key().Group != "DEFAULT_GROUP" {
			t.Fatal("requested raw identity lost")
		}
	}
	if _, err := client.ReadAll(context.Background()); !errors.Is(err, ErrEmpty) {
		t.Fatal("required ReadAll compatibility changed", err)
	}
}

func TestMetadataWatchDoesNotRetainARawBatch(t *testing.T) {
	fixture := newFixture(t, false)
	options := fixture.options()
	fixture.mu.Lock()
	for index := range 5 {
		name := fmt.Sprintf("raw-%d", index)
		if index == 0 {
			name = "settings.yaml"
		} else {
			options.Keys = append(options.Keys, KeyV1{DataID: name})
		}
		fixture.values[key{"DEFAULT_GROUP", name}] = strings.Repeat("x", MaxDocumentBytes)
	}
	fixture.mu.Unlock()
	client := openClient(t, options)
	if documents, _, err := client.ReadRawAll(context.Background()); documents != nil || !errors.Is(err, ErrLimit) {
		t.Fatal("raw batch aggregate bound missing", err)
	}
	subscription, err := client.Watch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := subscription.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for {
		change := nextChange(t, subscription)
		if change.Err() != nil {
			t.Fatal("metadata-only watch inherited raw retention limit", change.Err())
		}
		if change.Resync() {
			break
		}
	}
}
