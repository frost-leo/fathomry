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

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func TestPolicyExplicitCompositionAndStrictSettings(t *testing.T) {
	options := Settings{Name: "offline", Brokers: []string{"127.0.0.1:9092"}, ClusterID: "synthetic", Topics: []string{"records"}, Plaintext: true}
	one, err := Recommend(options)
	if err != nil {
		t.Fatal(err)
	}
	if one.Budget.WorkBytes <= 64<<20 {
		t.Fatal("policy silently used generic runtime allowance")
	}
	two, err := Compose(options, options)
	if err != nil {
		t.Fatal(err)
	}
	if two.Runtime.MaxActive != 2*one.Runtime.MaxActive || two.Runtime.MaxWorkBytes != 2*one.Runtime.MaxWorkBytes || two.Evidence.Capacity != 2*one.Evidence.Capacity {
		t.Fatal("overlapping source policy not composed")
	}
	if _, err := Compose(); !errors.Is(err, ErrInput) {
		t.Fatal("missing source count accepted")
	}
	larger := options
	larger.MaxBatchBytes, larger.MaxWireBytes = 8<<20, 16<<20
	mixed, err := Compose(options, larger)
	if err != nil {
		t.Fatal(err)
	}
	if mixed.Runtime.MaxWorkBytes < mixed.SourceWorkBytes+int64(mixed.Runtime.MaxActive-2)*mixed.Budget.WorkBytes || mixed.Evidence.MaxBytes < 2*sourceEvidenceBytes+int64(mixed.Evidence.Capacity-2)*mixed.Budget.EvidenceBytes {
		t.Fatal("heterogeneous generations undercharged their shared Using budget")
	}
	// Settings must stay plain-loadable; custom JSON/Text decoders are not added.
	value := reflect.TypeFor[Settings]()
	if value.Implements(reflect.TypeFor[json.Marshaler]()) || reflect.PointerTo(value).Implements(reflect.TypeFor[json.Unmarshaler]()) {
		t.Fatal("settings acquired runtime codecs")
	}
	snapshot, err := settings.New(options, func(value Settings) Settings {
		value.Brokers = append([]string{}, value.Brokers...)
		value.Topics = append([]string{}, value.Topics...)
		return value
	})
	if err != nil {
		t.Fatal(err)
	}
	options.Topics[0] = "mutated"
	frozen, err := snapshot.ValueCopy()
	if err != nil || frozen.Topics[0] != "records" {
		t.Fatal("settings alias escaped")
	}
	generic, err := adapters.New(context.Background(), adapters.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer generic.Close(context.Background())
	inbox, err := adapters.NewInbox[Result](one.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	if owner, err := Open(context.Background(), frozen, Dependencies{Runtime: generic, Evidence: inbox}); owner != nil || !errors.Is(err, adapters.ErrLimit) {
		t.Fatal("generic default reached native I/O", err)
	}
}
