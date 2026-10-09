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

package otel

import (
	"context"
	"github.com/frost-leo/fathomry/internal/compatibility"
	"slices"
	"time"
)

// BudgetInfo describes the exact prepared native envelopes before acquisition.
// QueueBytes is shared by pending logs, sampled ended spans and live spans.
// SpanBytes is part of that queue, not an additional reservation. MaxActiveSpans
// assumes no competing calls/evidence/queued data; no export slot is reserved.
type BudgetInfo struct {
	ActiveCalls, QueuedCalls                                                  int
	WorkBytes, EvidenceBytes, SourceBytes                                     int64
	ConfigurationBytes, QueueStorageBytes, MetricStorageBytes, TransportBytes int64
	QueueItems, QueueBytes, MaxActiveSpans                                    int
	SpanBytes                                                                 int64
	MaxRecordBytes, MaxRequestBytes, MaxResponseBytes                         int
	BatchSize, Instruments, MetricCardinality, MaxConnections                 int
	Timeout                                                                   time.Duration
	Logs, Traces, Metrics                                                     bool
}

// Metadata returns detached native costs and limits. Policy separately adds the
// public bridge, source-lifetime evidence and optional export-loop overhead.
func (prepared Prepared) Metadata() BudgetInfo {
	value := prepared.metadata
	return BudgetInfo{ActiveCalls: value.Limits.Active, QueuedCalls: value.Limits.Queued, WorkBytes: value.WorkBytes, EvidenceBytes: value.EvidenceBytes, SourceBytes: value.SourceBytes,
		ConfigurationBytes: value.ConfigurationBytes, QueueStorageBytes: value.QueueStorageBytes, MetricStorageBytes: value.MetricStorageBytes, TransportBytes: value.TransportBytes,
		QueueItems: value.QueueItems, QueueBytes: value.QueueBytes, MaxActiveSpans: value.MaxActiveSpans, SpanBytes: value.SpanBytes,
		MaxRecordBytes: value.MaxRecordBytes, MaxRequestBytes: value.MaxRequestBytes, MaxResponseBytes: value.MaxResponseBytes,
		BatchSize: value.BatchSize, Instruments: value.Instruments, MetricCardinality: value.MetricCardinality, MaxConnections: value.MaxConnections, Timeout: value.Timeout, Logs: value.Logs, Traces: value.Traces, Metrics: value.Metrics}
}

// Fact separates unknown, declared, observed and not-applicable information.
type Fact struct {
	private
	Kind, Value string
}

// Option is one effective non-secret selection, not a compatibility certificate.
type Option struct{ Name, Value string }

// Profile is detached frozen selection, not backend readiness or deployment
// certification. Library/SDK versions are independent from configuration format.
type Profile struct {
	private
	ImplementationModule, SDKMode                 string
	ServiceMode, ServiceVersion, Protocol, Native Fact
	Options                                       []Option
}

func (value Profile) Clone() Profile     { value.Options = slices.Clone(value.Options); return value }
func fact(value compatibility.Fact) Fact { return Fact{Kind: string(value.Kind), Value: value.Value} }

// Info returns frozen construction identity, not a resource-borrow generation.
func (owner *Owner) Info() Info { return owner.Handle().Info() }

// Info returns detached identity or zero for an absent token, without native work.
func (handle Handle) Info() Info {
	if handle.state == nil {
		return Info{}
	}
	return info(handle.state.info)
}

// Profile captures the currently borrowed source without native work. It does
// not promise the same Follow generation or capacity for a later operation.
func (client *Client) Profile(ctx context.Context) (Profile, error) {
	if client == nil || client.lifetime == nil || ctx == nil {
		return Profile{}, fail(ErrInput, "profile")
	}
	if ctx.Err() != nil {
		return Profile{}, fail(ErrState, "profile", ctx.Err(), context.Cause(ctx))
	}
	if client.lifetime.Err() != nil {
		return Profile{}, fail(ErrState, "profile", client.lifetime.Err(), context.Cause(client.lifetime))
	}
	handle := client.direct
	if client.source != nil {
		lease, err := client.source.Acquire(ctx)
		if err != nil {
			return Profile{}, err
		}
		defer lease.Release()
		handle, err = lease.Value()
		if err != nil {
			return Profile{}, err
		}
	}
	if handle.state == nil || handle.state.inspection == nil || handle.state.call.Context().Err() != nil {
		return Profile{}, fail(ErrState, "profile")
	}
	value := handle.state.inspection.Profile()
	result := Profile{ImplementationModule: value.ImplementationModule, SDKMode: value.SDKMode, ServiceMode: fact(value.ServiceMode), ServiceVersion: fact(value.ServiceVersion), Protocol: fact(value.Protocol), Native: fact(value.Native)}
	for _, item := range value.Options {
		result.Options = append(result.Options, Option{Name: item.Name, Value: item.Value})
	}
	return result, nil
}
