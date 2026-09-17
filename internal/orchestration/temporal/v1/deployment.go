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
	"slices"
	"strings"

	"github.com/frost-leo/fathomry/internal/fault"
	sdk "go.temporal.io/sdk/client"
)

// WorkerDeployment binds the native experimental deployment handle to one source.
// Routing changes are operational authority, not business build attestation.
// Native conflict tokens, manager identity and drainage/readiness errors remain
// intact. Metadata/results are caller-owned; no handle owns source release.
type WorkerDeployment struct {
	private
	client *Executions
	name   string
	native sdk.WorkerDeploymentHandle
}

func (*WorkerDeployment) LogValue() slog.Value { return slog.StringValue("temporal[restricted]") }

func (client *Executions) GetWorkerDeployment(name string) (*WorkerDeployment, error) {
	if client == nil || client.owner == nil || !validText(name, 255) {
		return nil, failure(ErrInput, "worker-deployment-handle")
	}
	return &WorkerDeployment{client: client, name: name, native: client.owner.native.WorkerDeploymentClient().GetHandle(name)}, nil
}

func operationalNative[T any](ctx context.Context, client *Executions, correlation fault.Correlation, evidence Execution, methods []string, invoke func(context.Context, *Execution) (T, error)) (T, error) {
	var zero T
	if client == nil || client.owner == nil {
		return zero, failure(ErrInput, evidence.Operation)
	}
	for _, method := range methods {
		if !slices.Contains(client.owner.settings.RPCs, "/temporal.api.workflowservice.v1.WorkflowService/"+method) {
			return zero, failure(ErrAuthority, evidence.Operation)
		}
	}
	return executeNative(ctx, client, correlation, evidence, invoke)
}

func deploymentCall[T any](ctx context.Context, handle *WorkerDeployment, correlation fault.Correlation, method string, invoke func(context.Context, sdk.WorkerDeploymentHandle) (T, error)) (T, error) {
	var zero T
	if handle == nil || handle.native == nil {
		return zero, failure(ErrInput, "worker-deployment")
	}
	return operationalNative(ctx, handle.client, correlation, Execution{Operation: "deployment." + strings.ToLower(method), DeploymentName: handle.name}, []string{method}, func(work context.Context, evidence *Execution) (T, error) {
		value, err := invoke(work, handle.native)
		if method == "DescribeWorkerDeployment" || method == "DescribeWorkerDeploymentVersion" {
			evidence.ResultObtained = err == nil
		} else {
			evidence.Accepted = err == nil
		}
		return value, err
	})
}

func (handle *WorkerDeployment) Describe(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentDescribeOptions) (sdk.WorkerDeploymentDescribeResponse, error) {
	return deploymentCall(ctx, handle, correlation, "DescribeWorkerDeployment", func(work context.Context, native sdk.WorkerDeploymentHandle) (sdk.WorkerDeploymentDescribeResponse, error) {
		return native.Describe(work, options)
	})
}

func (handle *WorkerDeployment) SetCurrentVersion(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentSetCurrentVersionOptions) (sdk.WorkerDeploymentSetCurrentVersionResponse, error) {
	return deploymentCall(ctx, handle, correlation, "SetWorkerDeploymentCurrentVersion", func(work context.Context, native sdk.WorkerDeploymentHandle) (sdk.WorkerDeploymentSetCurrentVersionResponse, error) {
		return native.SetCurrentVersion(work, options)
	})
}

func (handle *WorkerDeployment) SetRampingVersion(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentSetRampingVersionOptions) (sdk.WorkerDeploymentSetRampingVersionResponse, error) {
	return deploymentCall(ctx, handle, correlation, "SetWorkerDeploymentRampingVersion", func(work context.Context, native sdk.WorkerDeploymentHandle) (sdk.WorkerDeploymentSetRampingVersionResponse, error) {
		return native.SetRampingVersion(work, options)
	})
}

func (handle *WorkerDeployment) SetManagerIdentity(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentSetManagerIdentityOptions) (sdk.WorkerDeploymentSetManagerIdentityResponse, error) {
	return deploymentCall(ctx, handle, correlation, "SetWorkerDeploymentManager", func(work context.Context, native sdk.WorkerDeploymentHandle) (sdk.WorkerDeploymentSetManagerIdentityResponse, error) {
		return native.SetManagerIdentity(work, options)
	})
}

func (handle *WorkerDeployment) DescribeVersion(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentDescribeVersionOptions) (sdk.WorkerDeploymentVersionDescription, error) {
	return deploymentCall(ctx, handle, correlation, "DescribeWorkerDeploymentVersion", func(work context.Context, native sdk.WorkerDeploymentHandle) (sdk.WorkerDeploymentVersionDescription, error) {
		return native.DescribeVersion(work, options)
	})
}

func (handle *WorkerDeployment) DeleteVersion(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentDeleteVersionOptions) (sdk.WorkerDeploymentDeleteVersionResponse, error) {
	return deploymentCall(ctx, handle, correlation, "DeleteWorkerDeploymentVersion", func(work context.Context, native sdk.WorkerDeploymentHandle) (sdk.WorkerDeploymentDeleteVersionResponse, error) {
		return native.DeleteVersion(work, options)
	})
}

func (handle *WorkerDeployment) UpdateVersionMetadata(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentUpdateVersionMetadataOptions) (sdk.WorkerDeploymentUpdateVersionMetadataResponse, error) {
	if handle == nil || handle.native == nil {
		return sdk.WorkerDeploymentUpdateVersionMetadataResponse{}, failure(ErrInput, "worker-deployment")
	}
	return operationalNative(ctx, handle.client, correlation, Execution{Operation: "deployment.metadata", DeploymentName: options.Version.DeploymentName}, []string{"UpdateWorkerDeploymentVersionMetadata"}, func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentUpdateVersionMetadataResponse, error) {
		value, err := handle.native.UpdateVersionMetadata(work, options)
		evidence.Accepted = err == nil
		return value, err
	})
}

func (client *Executions) DeleteWorkerDeployment(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentDeleteOptions) (sdk.WorkerDeploymentDeleteResponse, error) {
	return operationalNative(ctx, client, correlation, Execution{Operation: "deployment.delete", DeploymentName: options.Name}, []string{"DeleteWorkerDeployment"}, func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentDeleteResponse, error) {
		result, err := client.owner.native.WorkerDeploymentClient().Delete(work, options)
		evidence.Accepted = err == nil
		return result, err
	})
}

// WalkWorkerDeployments visits native metadata without exporting a context-bound
// iterator. Empty continuation pages are not treated as exhaustion.
func (client *Executions) WalkWorkerDeployments(ctx context.Context, correlation fault.Correlation, options sdk.WorkerDeploymentListOptions, visit func(context.Context, *sdk.WorkerDeploymentListEntry) error) error {
	if visit == nil {
		return failure(ErrInput, "deployment-visitor")
	}
	_, err := operationalNative(ctx, client, correlation, Execution{Operation: "deployment.list"}, []string{"ListWorkerDeployments"}, func(work context.Context, evidence *Execution) (struct{}, error) {
		page := &schedulePage{}
		work = context.WithValue(work, schedulePageKey{}, page)
		iterator, err := client.owner.native.WorkerDeploymentClient().List(work, options)
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

type callbackWorkerDeploymentClient struct{ borrower *callbackClient }

type callbackWorkerDeployment struct {
	borrower *callbackClient
	name     string
}

func (client *callbackClient) WorkerDeploymentClient() sdk.WorkerDeploymentClient {
	return &callbackWorkerDeploymentClient{borrower: client}
}

func (client *callbackWorkerDeploymentClient) GetHandle(name string) sdk.WorkerDeploymentHandle {
	return &callbackWorkerDeployment{borrower: client.borrower, name: name}
}

func (client *callbackWorkerDeploymentClient) Delete(ctx context.Context, options sdk.WorkerDeploymentDeleteOptions) (sdk.WorkerDeploymentDeleteResponse, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"DeleteWorkerDeployment", Execution{Operation: "callback.deployment.delete", DeploymentName: options.Name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentDeleteResponse, error) {
			value, err := client.borrower.nativeClient.WorkerDeploymentClient().Delete(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (client *callbackWorkerDeploymentClient) List(ctx context.Context, options sdk.WorkerDeploymentListOptions) (sdk.WorkerDeploymentListIterator, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"ListWorkerDeployments", Execution{Operation: "callback.deployment.list"},
		func(_ context.Context, _ *Execution) (sdk.WorkerDeploymentListIterator, error) {
			iterator, err := client.borrower.nativeClient.WorkerDeploymentClient().List(client.borrower.iteratorContext(ctx), options)
			if err != nil {
				return nil, err
			}
			return newCallbackIterator(client.borrower, ctx, "deployment.list", workflowServicePrefix+"ListWorkerDeployments", iterator), nil
		})
}

func (handle *callbackWorkerDeployment) Describe(ctx context.Context, options sdk.WorkerDeploymentDescribeOptions) (sdk.WorkerDeploymentDescribeResponse, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"DescribeWorkerDeployment", Execution{Operation: "callback.deployment.describe", DeploymentName: handle.name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentDescribeResponse, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).Describe(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (handle *callbackWorkerDeployment) SetCurrentVersion(ctx context.Context, options sdk.WorkerDeploymentSetCurrentVersionOptions) (sdk.WorkerDeploymentSetCurrentVersionResponse, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"SetWorkerDeploymentCurrentVersion", Execution{Operation: "callback.deployment.setcurrentversion", DeploymentName: handle.name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentSetCurrentVersionResponse, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).SetCurrentVersion(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (handle *callbackWorkerDeployment) SetRampingVersion(ctx context.Context, options sdk.WorkerDeploymentSetRampingVersionOptions) (sdk.WorkerDeploymentSetRampingVersionResponse, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"SetWorkerDeploymentRampingVersion", Execution{Operation: "callback.deployment.setrampingversion", DeploymentName: handle.name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentSetRampingVersionResponse, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).SetRampingVersion(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (handle *callbackWorkerDeployment) SetManagerIdentity(ctx context.Context, options sdk.WorkerDeploymentSetManagerIdentityOptions) (sdk.WorkerDeploymentSetManagerIdentityResponse, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"SetWorkerDeploymentManager", Execution{Operation: "callback.deployment.setmanageridentity", DeploymentName: handle.name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentSetManagerIdentityResponse, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).SetManagerIdentity(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (handle *callbackWorkerDeployment) DescribeVersion(ctx context.Context, options sdk.WorkerDeploymentDescribeVersionOptions) (sdk.WorkerDeploymentVersionDescription, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"DescribeWorkerDeploymentVersion", Execution{Operation: "callback.deployment.describeversion", DeploymentName: handle.name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentVersionDescription, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).DescribeVersion(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (handle *callbackWorkerDeployment) DeleteVersion(ctx context.Context, options sdk.WorkerDeploymentDeleteVersionOptions) (sdk.WorkerDeploymentDeleteVersionResponse, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"DeleteWorkerDeploymentVersion", Execution{Operation: "callback.deployment.deleteversion", DeploymentName: handle.name},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentDeleteVersionResponse, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).DeleteVersion(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (handle *callbackWorkerDeployment) UpdateVersionMetadata(ctx context.Context, options sdk.WorkerDeploymentUpdateVersionMetadataOptions) (sdk.WorkerDeploymentUpdateVersionMetadataResponse, error) {
	return callbackGranted(ctx, handle.borrower, workflowServicePrefix+"UpdateWorkerDeploymentVersionMetadata", Execution{Operation: "callback.deployment.updateversionmetadata", DeploymentName: options.Version.DeploymentName},
		func(work context.Context, evidence *Execution) (sdk.WorkerDeploymentUpdateVersionMetadataResponse, error) {
			value, err := handle.borrower.nativeClient.WorkerDeploymentClient().GetHandle(handle.name).UpdateVersionMetadata(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

type callbackLegacyDeployment struct{ borrower *callbackClient }

func (client *callbackClient) DeploymentClient() sdk.DeploymentClient {
	return &callbackLegacyDeployment{borrower: client}
}

func (client *callbackLegacyDeployment) Describe(ctx context.Context, options sdk.DeploymentDescribeOptions) (sdk.DeploymentDescription, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"DescribeDeployment", Execution{Operation: "callback.legacy-deployment.describe"},
		func(work context.Context, evidence *Execution) (sdk.DeploymentDescription, error) {
			value, err := client.borrower.nativeClient.DeploymentClient().Describe(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackLegacyDeployment) GetReachability(ctx context.Context, options sdk.DeploymentGetReachabilityOptions) (sdk.DeploymentReachabilityInfo, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"GetDeploymentReachability", Execution{Operation: "callback.legacy-deployment.getreachability"},
		func(work context.Context, evidence *Execution) (sdk.DeploymentReachabilityInfo, error) {
			value, err := client.borrower.nativeClient.DeploymentClient().GetReachability(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackLegacyDeployment) GetCurrent(ctx context.Context, options sdk.DeploymentGetCurrentOptions) (sdk.DeploymentGetCurrentResponse, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"GetCurrentDeployment", Execution{Operation: "callback.legacy-deployment.getcurrent"},
		func(work context.Context, evidence *Execution) (sdk.DeploymentGetCurrentResponse, error) {
			value, err := client.borrower.nativeClient.DeploymentClient().GetCurrent(work, options)
			evidence.ResultObtained = err == nil
			return value, err
		})
}

func (client *callbackLegacyDeployment) SetCurrent(ctx context.Context, options sdk.DeploymentSetCurrentOptions) (sdk.DeploymentSetCurrentResponse, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"SetCurrentDeployment", Execution{Operation: "callback.legacy-deployment.setcurrent"},
		func(work context.Context, evidence *Execution) (sdk.DeploymentSetCurrentResponse, error) {
			value, err := client.borrower.nativeClient.DeploymentClient().SetCurrent(work, options)
			evidence.Accepted = err == nil
			return value, err
		})
}

func (client *callbackLegacyDeployment) List(ctx context.Context, options sdk.DeploymentListOptions) (sdk.DeploymentListIterator, error) {
	return callbackGranted(ctx, client.borrower, workflowServicePrefix+"ListDeployments", Execution{Operation: "callback.legacy-deployment.list"},
		func(context.Context, *Execution) (sdk.DeploymentListIterator, error) {
			native, err := client.borrower.nativeClient.DeploymentClient().List(client.borrower.iteratorContext(ctx), options)
			if err != nil {
				return nil, err
			}
			return newCallbackIterator(client.borrower, ctx, "legacy-deployment.list", workflowServicePrefix+"ListDeployments", native), nil
		})
}
