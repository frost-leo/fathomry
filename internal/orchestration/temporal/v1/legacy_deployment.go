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

package temporal

import (
	"context"
	"log/slog"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.temporal.io/sdk/client"
)

// DeploymentClient retains one source's legacy native deployment semantics.
// Each operation requires its exact operational RPC grant. Results are owned
// data, and the facade cannot release or replace its source.
//
// Deprecated: use WorkerDeployment.
type DeploymentClient struct {
	private
	client *Executions
}

func (*DeploymentClient) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

// DeploymentClient creates a local legacy facade without entering native code.
//
// Deprecated: use GetWorkerDeployment and WalkWorkerDeployments.
func (client *Executions) DeploymentClient() *DeploymentClient {
	return &DeploymentClient{client: client}
}

func legacyDeploymentCall[T any](ctx context.Context, client *DeploymentClient, correlation fault.Correlation, method string, invoke func(context.Context, *Execution, sdk.DeploymentClient) (T, error)) (T, error) {
	if client == nil {
		var zero T
		return zero, failure(ErrInput, "legacy-deployment")
	}
	return operationalNative(ctx, client.client, correlation, Execution{Operation: "legacy-deployment." + strings.ToLower(method)}, []string{method},
		func(work context.Context, evidence *Execution) (T, error) {
			return invoke(work, evidence, client.client.owner.native.DeploymentClient())
		})
}

func (client *DeploymentClient) Describe(ctx context.Context, correlation fault.Correlation, options sdk.DeploymentDescribeOptions) (sdk.DeploymentDescription, error) {
	return legacyDeploymentCall(ctx, client, correlation, "DescribeDeployment", func(work context.Context, evidence *Execution, native sdk.DeploymentClient) (sdk.DeploymentDescription, error) {
		value, err := native.Describe(work, options)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

func (client *DeploymentClient) GetReachability(ctx context.Context, correlation fault.Correlation, options sdk.DeploymentGetReachabilityOptions) (sdk.DeploymentReachabilityInfo, error) {
	return legacyDeploymentCall(ctx, client, correlation, "GetDeploymentReachability", func(work context.Context, evidence *Execution, native sdk.DeploymentClient) (sdk.DeploymentReachabilityInfo, error) {
		value, err := native.GetReachability(work, options)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

func (client *DeploymentClient) GetCurrent(ctx context.Context, correlation fault.Correlation, options sdk.DeploymentGetCurrentOptions) (sdk.DeploymentGetCurrentResponse, error) {
	return legacyDeploymentCall(ctx, client, correlation, "GetCurrentDeployment", func(work context.Context, evidence *Execution, native sdk.DeploymentClient) (sdk.DeploymentGetCurrentResponse, error) {
		value, err := native.GetCurrent(work, options)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

func (client *DeploymentClient) SetCurrent(ctx context.Context, correlation fault.Correlation, options sdk.DeploymentSetCurrentOptions) (sdk.DeploymentSetCurrentResponse, error) {
	return legacyDeploymentCall(ctx, client, correlation, "SetCurrentDeployment", func(work context.Context, evidence *Execution, native sdk.DeploymentClient) (sdk.DeploymentSetCurrentResponse, error) {
		value, err := native.SetCurrent(work, options)
		evidence.Accepted = err == nil
		return value, err
	})
}

// Walk owns native List pagination and synchronous visits within one admitted
// operation. Empty continuation pages are not mistaken for exhaustion.
func (client *DeploymentClient) Walk(ctx context.Context, correlation fault.Correlation, options sdk.DeploymentListOptions, visit func(context.Context, *sdk.DeploymentListEntry) error) error {
	if visit == nil {
		return failure(ErrInput, "legacy-deployment-visitor")
	}
	_, err := legacyDeploymentCall(ctx, client, correlation, "ListDeployments", func(work context.Context, evidence *Execution, native sdk.DeploymentClient) (struct{}, error) {
		page := &schedulePage{}
		work = context.WithValue(work, schedulePageKey{}, page)
		iterator, err := native.List(work, options)
		if err != nil {
			return struct{}{}, err
		}
		for {
			if err := work.Err(); err != nil {
				return struct{}{}, err
			}
			if !iterator.HasNext() {
				if page.more {
					continue
				}
				break
			}
			entry, err := iterator.Next()
			if err != nil {
				return struct{}{}, err
			}
			if err := visit(work, entry); err != nil {
				return struct{}{}, err
			}
		}
		evidence.ResultObtained = true
		return struct{}{}, nil
	})
	return err
}
