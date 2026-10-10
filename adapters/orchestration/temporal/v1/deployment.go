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
	context "context"
	native "github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	sdk "go.temporal.io/sdk/client"
)

// WorkerDeployment retains its originating Client identity and native semantics.
type WorkerDeployment struct {
	private
	native *native.WorkerDeployment
	client *Client
}

func wrapWorkerDeployment(value *native.WorkerDeployment, client *Client) *WorkerDeployment {
	if value == nil {
		return nil
	}
	return &WorkerDeployment{native: value, client: client}
}

// DeploymentClient retains its originating Client identity and native semantics.
// Deprecated: use WorkerDeployment.
type DeploymentClient struct {
	private
	native *native.DeploymentClient
	client *Client
}

func wrapDeploymentClient(value *native.DeploymentClient, client *Client) *DeploymentClient {
	if value == nil {
		return nil
	}
	return &DeploymentClient{native: value, client: client}
}

// GetWorkerDeployment preserves the selected native contract within this retained use.
func (view *Client) GetWorkerDeployment(name string) (*WorkerDeployment, error) {
	value, err := view.executions().GetWorkerDeployment(name)
	return wrapWorkerDeployment(value, view), translate(err, "getworkerdeployment")
}

// Describe preserves the selected native contract within this retained use.
func (view *WorkerDeployment) Describe(ctx context.Context, options sdk.WorkerDeploymentDescribeOptions) (sdk.WorkerDeploymentDescribeResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentDescribeResponse
		return zero, fail(ErrInput, "describe")
	}
	value, err := view.native.Describe(ctx, view.client.correlation(), options)
	return value, translate(err, "describe")
}

// SetCurrentVersion preserves the selected native contract within this retained use.
func (view *WorkerDeployment) SetCurrentVersion(ctx context.Context, options sdk.WorkerDeploymentSetCurrentVersionOptions) (sdk.WorkerDeploymentSetCurrentVersionResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentSetCurrentVersionResponse
		return zero, fail(ErrInput, "setcurrentversion")
	}
	value, err := view.native.SetCurrentVersion(ctx, view.client.correlation(), options)
	return value, translate(err, "setcurrentversion")
}

// SetRampingVersion preserves the selected native contract within this retained use.
func (view *WorkerDeployment) SetRampingVersion(ctx context.Context, options sdk.WorkerDeploymentSetRampingVersionOptions) (sdk.WorkerDeploymentSetRampingVersionResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentSetRampingVersionResponse
		return zero, fail(ErrInput, "setrampingversion")
	}
	value, err := view.native.SetRampingVersion(ctx, view.client.correlation(), options)
	return value, translate(err, "setrampingversion")
}

// SetManagerIdentity preserves the selected native contract within this retained use.
func (view *WorkerDeployment) SetManagerIdentity(ctx context.Context, options sdk.WorkerDeploymentSetManagerIdentityOptions) (sdk.WorkerDeploymentSetManagerIdentityResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentSetManagerIdentityResponse
		return zero, fail(ErrInput, "setmanageridentity")
	}
	value, err := view.native.SetManagerIdentity(ctx, view.client.correlation(), options)
	return value, translate(err, "setmanageridentity")
}

// DescribeVersion preserves the selected native contract within this retained use.
func (view *WorkerDeployment) DescribeVersion(ctx context.Context, options sdk.WorkerDeploymentDescribeVersionOptions) (sdk.WorkerDeploymentVersionDescription, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentVersionDescription
		return zero, fail(ErrInput, "describeversion")
	}
	value, err := view.native.DescribeVersion(ctx, view.client.correlation(), options)
	return value, translate(err, "describeversion")
}

// DeleteVersion preserves the selected native contract within this retained use.
func (view *WorkerDeployment) DeleteVersion(ctx context.Context, options sdk.WorkerDeploymentDeleteVersionOptions) (sdk.WorkerDeploymentDeleteVersionResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentDeleteVersionResponse
		return zero, fail(ErrInput, "deleteversion")
	}
	value, err := view.native.DeleteVersion(ctx, view.client.correlation(), options)
	return value, translate(err, "deleteversion")
}

// UpdateVersionMetadata preserves the selected native contract within this retained use.
func (view *WorkerDeployment) UpdateVersionMetadata(ctx context.Context, options sdk.WorkerDeploymentUpdateVersionMetadataOptions) (sdk.WorkerDeploymentUpdateVersionMetadataResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.WorkerDeploymentUpdateVersionMetadataResponse
		return zero, fail(ErrInput, "updateversionmetadata")
	}
	value, err := view.native.UpdateVersionMetadata(ctx, view.client.correlation(), options)
	return value, translate(err, "updateversionmetadata")
}

// DeleteWorkerDeployment preserves the selected native contract within this retained use.
func (view *Client) DeleteWorkerDeployment(ctx context.Context, options sdk.WorkerDeploymentDeleteOptions) (sdk.WorkerDeploymentDeleteResponse, error) {
	value, err := view.executions().DeleteWorkerDeployment(ctx, view.correlation(), options)
	return value, translate(err, "deleteworkerdeployment")
}

// WalkWorkerDeployments visits native metadata without exporting a context-bound
// iterator. Empty continuation pages are not treated as exhaustion.
func (view *Client) WalkWorkerDeployments(ctx context.Context, options sdk.WorkerDeploymentListOptions, visit func(context.Context, *sdk.WorkerDeploymentListEntry) error) error {
	return translate(view.executions().WalkWorkerDeployments(ctx, view.correlation(), options, visit), "walkworkerdeployments")
}

// DeploymentClient creates a local legacy facade without entering native code.
//
// Deprecated: use GetWorkerDeployment and WalkWorkerDeployments.
func (view *Client) DeploymentClient() *DeploymentClient {
	return wrapDeploymentClient(view.executions().DeploymentClient(), view)
}

// Describe preserves the selected native contract within this retained use.
func (view *DeploymentClient) Describe(ctx context.Context, options sdk.DeploymentDescribeOptions) (sdk.DeploymentDescription, error) {
	if view == nil || view.native == nil {
		var zero sdk.DeploymentDescription
		return zero, fail(ErrInput, "describe")
	}
	value, err := view.native.Describe(ctx, view.client.correlation(), options)
	return value, translate(err, "describe")
}

// GetReachability preserves the selected native contract within this retained use.
func (view *DeploymentClient) GetReachability(ctx context.Context, options sdk.DeploymentGetReachabilityOptions) (sdk.DeploymentReachabilityInfo, error) {
	if view == nil || view.native == nil {
		var zero sdk.DeploymentReachabilityInfo
		return zero, fail(ErrInput, "getreachability")
	}
	value, err := view.native.GetReachability(ctx, view.client.correlation(), options)
	return value, translate(err, "getreachability")
}

// GetCurrent preserves the selected native contract within this retained use.
func (view *DeploymentClient) GetCurrent(ctx context.Context, options sdk.DeploymentGetCurrentOptions) (sdk.DeploymentGetCurrentResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.DeploymentGetCurrentResponse
		return zero, fail(ErrInput, "getcurrent")
	}
	value, err := view.native.GetCurrent(ctx, view.client.correlation(), options)
	return value, translate(err, "getcurrent")
}

// SetCurrent preserves the selected native contract within this retained use.
func (view *DeploymentClient) SetCurrent(ctx context.Context, options sdk.DeploymentSetCurrentOptions) (sdk.DeploymentSetCurrentResponse, error) {
	if view == nil || view.native == nil {
		var zero sdk.DeploymentSetCurrentResponse
		return zero, fail(ErrInput, "setcurrent")
	}
	value, err := view.native.SetCurrent(ctx, view.client.correlation(), options)
	return value, translate(err, "setcurrent")
}

// Walk owns native List pagination and synchronous visits within one admitted
// operation. Empty continuation pages are not mistaken for exhaustion.
func (view *DeploymentClient) Walk(ctx context.Context, options sdk.DeploymentListOptions, visit func(context.Context, *sdk.DeploymentListEntry) error) error {
	if view == nil || view.native == nil {
		return fail(ErrInput, "walk")
	}
	return translate(view.native.Walk(ctx, view.client.correlation(), options, visit), "walk")
}

// GetWorkerBuildIdCompatibility preserves native validation and converted version
// sets under its exact RPC grant.
//
// Deprecated: use Worker Deployment versioning.
func (view *Client) GetWorkerBuildIdCompatibility(ctx context.Context, options *sdk.GetWorkerBuildIdCompatibilityOptions) (*sdk.WorkerBuildIDVersionSets, error) {
	value, err := view.executions().GetWorkerBuildIdCompatibility(ctx, view.correlation(), options)
	return value, translate(err, "getworkerbuildidcompatibility")
}

// GetWorkerTaskReachability preserves native reachability conversion and the
// distinction between retrieved task queues and task queues not retrieved.
//
// Deprecated: use Worker Deployment versioning.
func (view *Client) GetWorkerTaskReachability(ctx context.Context, options *sdk.GetWorkerTaskReachabilityOptions) (*sdk.WorkerTaskReachability, error) {
	value, err := view.executions().GetWorkerTaskReachability(ctx, view.correlation(), options)
	return value, translate(err, "getworkertaskreachability")
}

// GetWorkerVersioningRules retains native assignment/redirect rules and conflict
// tokens. Its result is caller-owned.
//
// Deprecated: use Worker Deployment versioning.
func (view *Client) GetWorkerVersioningRules(ctx context.Context, options sdk.GetWorkerVersioningOptions) (*sdk.WorkerVersioningRules, error) {
	value, err := view.executions().GetWorkerVersioningRules(ctx, view.correlation(), options)
	return value, translate(err, "getworkerversioningrules")
}

// UpdateWorkerBuildIdCompatibility preserves the native operation validation
// and protocol conversion under its exact RPC grant.
//
// Deprecated: use Worker Deployment versioning.
func (view *Client) UpdateWorkerBuildIdCompatibility(ctx context.Context, options *sdk.UpdateWorkerBuildIdCompatibilityOptions) error {
	return translate(view.executions().UpdateWorkerBuildIdCompatibility(ctx, view.correlation(), options), "updateworkerbuildidcompatibility")
}

// UpdateWorkerVersioningRules preserves native operation validation, conflict
// tokens and response conversion under its exact RPC grant.
//
// Deprecated: use Worker Deployment versioning.
func (view *Client) UpdateWorkerVersioningRules(ctx context.Context, options sdk.UpdateWorkerVersioningRulesOptions) (*sdk.WorkerVersioningRules, error) {
	value, err := view.executions().UpdateWorkerVersioningRules(ctx, view.correlation(), options)
	return value, translate(err, "updateworkerversioningrules")
}
