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

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.temporal.io/sdk/client"
)

// GetWorkerBuildIdCompatibility preserves native validation and converted version
// sets under its exact RPC grant.
//
// Deprecated: use Worker Deployment versioning.
func (client *Executions) GetWorkerBuildIdCompatibility(ctx context.Context, correlation fault.Correlation, options *sdk.GetWorkerBuildIdCompatibilityOptions) (*sdk.WorkerBuildIDVersionSets, error) {
	if options == nil {
		return nil, failure(ErrInput, "worker-build-compatibility")
	}
	return operationalNative(ctx, client, correlation, Execution{Operation: "worker.build-compatibility"}, []string{"GetWorkerBuildIdCompatibility"}, func(work context.Context, evidence *Execution) (*sdk.WorkerBuildIDVersionSets, error) {
		value, err := client.owner.native.GetWorkerBuildIdCompatibility(work, options)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

// GetWorkerTaskReachability preserves native reachability conversion and the
// distinction between retrieved task queues and task queues not retrieved.
//
// Deprecated: use Worker Deployment versioning.
func (client *Executions) GetWorkerTaskReachability(ctx context.Context, correlation fault.Correlation, options *sdk.GetWorkerTaskReachabilityOptions) (*sdk.WorkerTaskReachability, error) {
	if options == nil {
		return nil, failure(ErrInput, "worker-task-reachability")
	}
	return operationalNative(ctx, client, correlation, Execution{Operation: "worker.task-reachability"}, []string{"GetWorkerTaskReachability"}, func(work context.Context, evidence *Execution) (*sdk.WorkerTaskReachability, error) {
		value, err := client.owner.native.GetWorkerTaskReachability(work, options)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

// GetWorkerVersioningRules retains native assignment/redirect rules and conflict
// tokens. Its result is caller-owned.
//
// Deprecated: use Worker Deployment versioning.
func (client *Executions) GetWorkerVersioningRules(ctx context.Context, correlation fault.Correlation, options sdk.GetWorkerVersioningOptions) (*sdk.WorkerVersioningRules, error) {
	return operationalNative(ctx, client, correlation, Execution{Operation: "worker.versioning-rules"}, []string{"GetWorkerVersioningRules"}, func(work context.Context, evidence *Execution) (*sdk.WorkerVersioningRules, error) {
		value, err := client.owner.native.GetWorkerVersioningRules(work, options)
		evidence.ResultObtained = err == nil
		return value, err
	})
}

// UpdateWorkerBuildIdCompatibility preserves the native operation validation
// and protocol conversion under its exact RPC grant.
//
// Deprecated: use Worker Deployment versioning.
func (client *Executions) UpdateWorkerBuildIdCompatibility(ctx context.Context, correlation fault.Correlation, options *sdk.UpdateWorkerBuildIdCompatibilityOptions) error {
	if options == nil {
		return failure(ErrInput, "worker-build-compatibility")
	}
	_, err := operationalNative(ctx, client, correlation, Execution{Operation: "worker.update-build-compatibility"}, []string{"UpdateWorkerBuildIdCompatibility"}, func(work context.Context, evidence *Execution) (struct{}, error) {
		err := client.owner.native.UpdateWorkerBuildIdCompatibility(work, options)
		evidence.Accepted = err == nil
		return struct{}{}, err
	})
	return err
}

// UpdateWorkerVersioningRules preserves native operation validation, conflict
// tokens and response conversion under its exact RPC grant.
//
// Deprecated: use Worker Deployment versioning.
func (client *Executions) UpdateWorkerVersioningRules(ctx context.Context, correlation fault.Correlation, options sdk.UpdateWorkerVersioningRulesOptions) (*sdk.WorkerVersioningRules, error) {
	return operationalNative(ctx, client, correlation, Execution{Operation: "worker.update-versioning-rules"}, []string{"UpdateWorkerVersioningRules"}, func(work context.Context, evidence *Execution) (*sdk.WorkerVersioningRules, error) {
		value, err := client.owner.native.UpdateWorkerVersioningRules(work, options)
		evidence.Accepted = err == nil
		return value, err
	})
}
