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

package configuration

import (
	"context"
	"errors"

	"github.com/frost-leo/fathomry/adapters/v1"
)

type scenario struct {
	runtime  *adapters.Runtime
	evidence *adapters.Inbox[Evidence]
	binding  sourceBinding
}

func newScenario(ctx context.Context) (*scenario, error) {
	inbox, err := adapters.NewInbox[Evidence](adapters.EvidenceOptions{Capacity: 4, MaxBytes: 256 << 10})
	if err != nil {
		return nil, err
	}
	runtime, err := adapters.New(ctx, adapters.Options{Name: "configuration", MaxActive: 4, MaxWorkBytes: 128 << 20})
	if err != nil {
		return nil, err
	}
	return &scenario{runtime: runtime, evidence: inbox}, nil
}
func (scenario *scenario) endpoint() (adapters.Endpoint[Evidence], error) {
	return adapters.Bind(scenario.runtime, adapters.Declaration[Evidence]{Evidence: scenario.evidence, Copy: func(value Evidence) Evidence { return value }})
}
func (scenario *scenario) open(ctx context.Context, provider Provider) error {
	var err error
	scenario.binding, err = provider.state.open(ctx, scenario.runtime)
	if err == nil && len(provider.state.layers) > 0 && nilInterface(scenario.binding.source) {
		return fail(ErrSource, "provider")
	}
	return err
}
func (scenario *scenario) finish(ctx context.Context) ([]Record, error) {
	var cleanup error
	if scenario.binding.close != nil {
		cleanup = scenario.binding.close(ctx)
	}
	cleanup = errors.Join(cleanup, scenario.runtime.Close(ctx))
	records, err := takeRecords(ctx, scenario.evidence, "configuration", func(value Evidence) recordFacts { return recordFacts{configuration: value, configurationPresent: true} })
	cleanup = errors.Join(cleanup, err)
	if scenario.binding.records != nil {
		source, err := scenario.binding.records(ctx)
		records = append(records, source...)
		cleanup = errors.Join(cleanup, err)
	}
	return records, cleanup
}
func request(operation string) adapters.Request {
	return adapters.Request{Operation: "configuration." + operation, WorkBytes: 16 << 20, EvidenceBytes: 64 << 10}
}
