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

package franz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/resource"
)

func TestOptionsValidationAndExplicitDefaults(t *testing.T) {
	valid := OptionsV1{Name: "data", Brokers: []string{"127.0.0.1:9092"}, ClusterID: "test", Topics: []string{"records"}, Plaintext: true}
	tests := []struct {
		name   string
		change func(*OptionsV1)
	}{
		{"missing-name", func(v *OptionsV1) { v.Name = "" }},
		{"missing-brokers", func(v *OptionsV1) { v.Brokers = nil }},
		{"dns", func(v *OptionsV1) { v.Brokers = []string{"localhost:9092"} }},
		{"zero-port", func(v *OptionsV1) { v.Brokers = []string{"127.0.0.1:0"} }},
		{"duplicate-brokers", func(v *OptionsV1) { v.Brokers = []string{"127.0.0.1:9092", "127.0.0.1:9092"} }},
		{"cluster", func(v *OptionsV1) { v.ClusterID = "" }},
		{"topic", func(v *OptionsV1) { v.Topics = []string{"bad/topic"} }},
		{"duplicate-topics", func(v *OptionsV1) { v.Topics = []string{"records", "records"} }},
		{"format", func(v *OptionsV1) { v.Version = 2 }},
		{"tls-required", func(v *OptionsV1) { v.Plaintext = false }},
		{"tls-conflict", func(v *OptionsV1) { v.RootCAPEM = "private-canary" }},
		{"negative-timeout", func(v *OptionsV1) { v.Timeout = -time.Second }},
		{"active", func(v *OptionsV1) { v.MaxActive = 17 }},
		{"negative-queue", func(v *OptionsV1) { v.QueuedCalls = -1 }},
		{"retries", func(v *OptionsV1) { v.Retries = 11 }},
		{"codec", func(v *OptionsV1) { v.Compression = "snappy" }},
		{"bytes", func(v *OptionsV1) { v.MaxBatchBytes = 1024 }},
		{"wire", func(v *OptionsV1) { v.MaxWireBytes = 1024 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := valid
			test.change(&options)
			if _, err := Select(options); !errors.Is(err, resource.ErrConfiguration) {
				t.Fatal("invalid configuration accepted", err)
			}
		})
	}
	value := defaults(valid)
	if value.Retries != 0 || value.Linger != 0 || value.Compression != "none" || value.MaxActive != 4 || value.MaxRecords != 256 {
		t.Fatal("hidden defaults")
	}
	for _, layer := range []string{"unknown: true", "acks: 0", "consumer_group: bad", "timeout_ns: 0", "max_active: 0", "max_records: -1", "compression: snappy", "brokers: null", "max_active: 1\nmax_active: 2"} {
		if _, err := Select(valid, resource.Layer{Kind: resource.Local, Content: []byte(layer)}); err == nil {
			t.Fatalf("invalid layer accepted: %s", layer)
		}
	}
	if _, err := Select(valid, resource.Layer{Kind: resource.Local, Content: []byte("retries: 0\nlinger_ns: 0\nqueued_calls: 0")}); err != nil {
		t.Fatal("explicit zero rejected", err)
	}
}
func TestPreparedSelectionCopyAndConcurrentReuse(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	layer := []byte("max_records: 8")
	selected, err := Select(options, resource.Layer{Kind: resource.Local, Content: layer})
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	options.Brokers[0] = "127.0.0.1:1"
	options.Topics[0] = "mutated"
	layer[0] = 'X'
	var wait sync.WaitGroup
	for range 4 {
		wait.Go(func() {
			assembly, err := resource.Assemble(deadline(t), deadline(t), "reuse", selected)
			if err != nil {
				t.Error(err)
				return
			}
			defer func() {
				if err := assembly.Close(deadline(t)); err != nil {
					t.Error(err)
				}
			}()
			source, _, err := resource.Bind(assembly, selected)
			if err != nil {
				t.Error(err)
				return
			}
			if source.owner.settings.MaxRecords != 8 || source.owner.settings.Topics[0] != "records" || source.owner.settings.Brokers[0] == "127.0.0.1:1" {
				t.Error("prepared alias or overwrite")
			}
		})
	}
	wait.Wait()
}
func TestAssemblyWrongClusterAndDuplicatePreflight(t *testing.T) {
	cluster := localCluster(t)
	options := clusterOptions(cluster)
	options.ClusterID = "not-the-cluster"
	selected, err := Select(options)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(options))
	assembly, err := resource.Assemble(deadline(t), deadline(t), "wrong", selected)
	if !errors.Is(err, ErrIdentity) {
		t.Fatal("wrong cluster accepted", err)
	}
	if assembly == nil {
		t.Fatal("cleanup owner lost")
	}
	if err := assembly.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	good := clusterOptions(cluster)
	selected, err = Select(good)
	if err != nil {
		t.Fatal(err)
	}
	selected = resource.WithLimits(selected, LimitsV1(good))
	if assembly, err := resource.Assemble(deadline(t), deadline(t), "duplicate", selected, selected); err == nil || assembly != nil {
		t.Fatal("duplicate selection reached constructors")
	}
}
