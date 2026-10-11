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
	"errors"
	"fmt"
	"time"

	"github.com/frost-leo/fathomry/adapters/tableformat/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
	fmt.Println("tableformat public contracts composed without native I/O")
}

func run() (resultErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := tableformat.Policy{
		Budget:   tableformat.Budget{WorkBytes: 1024, EvidenceBytes: 512, SourceWorkBytes: 2048, SourceEvidenceBytes: 256},
		Runtime:  adapters.Options{MaxActive: 2, MaxWorkBytes: 3072},
		Evidence: adapters.EvidenceOptions{Capacity: 2, MaxBytes: 768},
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, runtime.Close(context.Background())) }()
	inbox, err := adapters.NewInbox[tableformat.Info](policy.Evidence)
	if err != nil {
		return err
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[tableformat.Info]{Evidence: inbox, Copy: func(value tableformat.Info) tableformat.Info { return value.Clone() }})
	if err != nil {
		return err
	}
	for _, request := range []adapters.Request{
		{Operation: "tableformat.source", WorkBytes: policy.Budget.SourceWorkBytes, EvidenceBytes: policy.Budget.SourceEvidenceBytes},
		{Operation: "tableformat.operation", WorkBytes: policy.Budget.WorkBytes, EvidenceBytes: policy.Budget.EvidenceBytes},
	} {
		receipt, err := endpoint.Run(ctx, request, func(call *adapters.Call[tableformat.Info]) {
			_ = call.Resolve(adapters.Outcome[tableformat.Info]{Present: true,
				Value: tableformat.Info{Name: request.Operation, Provenance: []tableformat.LayerInfo{{Fields: []string{"limit"}}}}})
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
		if !present || value.Name != request.Operation {
			return fmt.Errorf("category metadata not delivered")
		}
		value.Provenance[0].Fields[0] = "changed"
	}
	entered := false
	_, err = endpoint.Run(ctx, adapters.Request{Operation: "tableformat.refused", WorkBytes: policy.Budget.WorkBytes, EvidenceBytes: policy.Budget.EvidenceBytes},
		func(*adapters.Call[tableformat.Info]) { entered = true })
	if !errors.Is(err, adapters.ErrEvidence) || entered {
		return fmt.Errorf("release was confused with evidence acknowledgement: %w", err)
	}
	for range 2 {
		delivery, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		receipt, err := delivery.Receipt()
		if err != nil {
			return err
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil {
			return err
		}
		value, present := snapshot.ValueCopy()
		if !present || value.Provenance[0].Fields[0] != "limit" {
			return fmt.Errorf("independent metadata custody aliases caller")
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}
