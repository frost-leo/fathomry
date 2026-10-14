/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	browser "github.com/frost-leo/fathomry/adapters/browser/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
	fmt.Println("browser public contracts composed without native I/O")
}
func run() (result error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	policy := browser.Policy{
		Budget:   browser.Budget{WorkBytes: 1024, EvidenceBytes: 512, SourceWorkBytes: 2048, SourceEvidenceBytes: 256},
		Runtime:  adapters.Options{MaxActive: 2, MaxWorkBytes: 3072},
		Evidence: adapters.EvidenceOptions{Capacity: 2, MaxBytes: 768},
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, runtime.Close(context.Background())) }()
	inbox, err := adapters.NewInbox[browser.Observation](policy.Evidence)
	if err != nil {
		return err
	}
	endpoint, err := adapters.Bind(runtime, adapters.Declaration[browser.Observation]{Evidence: inbox, Copy: func(value browser.Observation) browser.Observation { return value }})
	if err != nil {
		return err
	}
	for _, request := range []adapters.Request{
		{Operation: "browser.source", WorkBytes: policy.Budget.SourceWorkBytes, EvidenceBytes: policy.Budget.SourceEvidenceBytes},
		{Operation: "browser.operation", WorkBytes: policy.Budget.WorkBytes, EvidenceBytes: policy.Budget.EvidenceBytes},
	} {
		receipt, err := endpoint.Run(ctx, request, func(call *adapters.Call[browser.Observation]) {
			_ = call.Resolve(adapters.Outcome[browser.Observation]{Present: true, Value: browser.Observation{Returned: true, Effect: browser.Unknown}})
		})
		if err != nil {
			return err
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil {
			return err
		}
		if snapshot.Err() != nil {
			return snapshot.Err()
		}
		copy, present := snapshot.ValueCopy()
		if !present || !copy.Returned || copy.Effect != browser.Unknown || copy.ProcessExited {
			return errors.New("observation axes conflated")
		}
		copy.Effect = browser.Acknowledged
	}
	entered := false
	_, err = endpoint.Run(ctx, adapters.Request{Operation: "browser.refused", WorkBytes: policy.Budget.WorkBytes, EvidenceBytes: policy.Budget.EvidenceBytes}, func(*adapters.Call[browser.Observation]) { entered = true })
	if !errors.Is(err, adapters.ErrEvidence) || entered {
		return errors.New("independent evidence did not backpressure admission")
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
		if !present || value.Effect != browser.Unknown {
			return errors.New("independent evidence aliases the direct copy")
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}
