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
	"context"
	"fmt"
	"time"

	"github.com/frost-leo/fathomry/adapters/sqlengine/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
	fmt.Println("sqlengine public contracts composed without native I/O")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := sqlengine.Policy{Budget: sqlengine.Budget{WorkBytes: 1024, EvidenceBytes: 1024},
		Runtime: adapters.Options{MaxActive: 2, MaxWorkBytes: 2048}, Evidence: adapters.EvidenceOptions{Capacity: 2, MaxBytes: 2048},
		SourceWorkBytes: 1024, SourceEvidenceBytes: 1024}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	inbox, err := adapters.NewInbox[sqlengine.Info](policy.Evidence)
	if err != nil {
		return err
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[sqlengine.Info]{Evidence: inbox, Copy: func(value sqlengine.Info) sqlengine.Info { return value.Clone() }})
	if err != nil {
		return err
	}
	receipt, err := endpoint.Run(ctx, adapters.Request{Operation: "sqlengine.contract", WorkBytes: policy.Budget.WorkBytes, EvidenceBytes: policy.Budget.EvidenceBytes},
		func(call *adapters.Call[sqlengine.Info]) {
			_ = call.Resolve(adapters.Outcome[sqlengine.Info]{Present: true, Value: sqlengine.Info{Name: "consumer", Provenance: []sqlengine.LayerInfo{{Fields: []string{"limit"}}}}})
		})
	if err != nil {
		return err
	}
	snapshot, err := receipt.WaitReleased(ctx)
	if err != nil {
		return err
	}
	if err := snapshot.Err(); err != nil {
		return err
	}
	value, present := snapshot.ValueCopy()
	if !present || value.Name != "consumer" {
		return fmt.Errorf("common metadata not delivered")
	}
	value.Provenance[0].Fields[0] = "changed"
	delivery, err := inbox.NextReleased(ctx)
	if err != nil {
		return err
	}
	retained, err := delivery.Receipt()
	if err != nil {
		return err
	}
	observation, err := retained.WaitReleased(ctx)
	if err != nil {
		return err
	}
	independent, present := observation.ValueCopy()
	if !present || independent.Provenance[0].Fields[0] != "limit" {
		return fmt.Errorf("independent metadata custody aliases caller")
	}
	return delivery.Ack()
}
