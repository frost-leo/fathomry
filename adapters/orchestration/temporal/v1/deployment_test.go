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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var deploymentCapabilityGrants = []string{
	"DescribeWorkerDeployment", "SetWorkerDeploymentCurrentVersion", "SetWorkerDeploymentRampingVersion", "SetWorkerDeploymentManager",
	"DescribeWorkerDeploymentVersion", "DeleteWorkerDeploymentVersion", "UpdateWorkerDeploymentVersionMetadata", "DeleteWorkerDeployment", "ListWorkerDeployments",
	"DescribeDeployment", "GetDeploymentReachability", "GetCurrentDeployment", "SetCurrentDeployment", "ListDeployments",
	"GetWorkerBuildIdCompatibility", "GetWorkerTaskReachability", "GetWorkerVersioningRules", "UpdateWorkerBuildIdCompatibility", "UpdateWorkerVersioningRules",
}

type deploymentCapabilityServer struct {
	testServer
	requests   chan proto.Message
	failMethod string
	failure    error
}

func newDeploymentCapabilityServer() *deploymentCapabilityServer {
	return &deploymentCapabilityServer{requests: make(chan proto.Message, 64)}
}

func (server *deploymentCapabilityServer) record(method string, request proto.Message) error {
	server.requests <- request
	if server.failMethod == method {
		return server.failure
	}
	return nil
}

func deploymentRequest[T proto.Message](t *testing.T, server *deploymentCapabilityServer) T {
	t.Helper()
	select {
	case message := <-server.requests:
		request, ok := message.(T)
		if !ok {
			t.Fatalf("unexpected native deployment request %T", message)
		}
		if namespaced, ok := message.(interface{ GetNamespace() string }); !ok || namespaced.GetNamespace() != "test" {
			t.Fatal("native deployment namespace was not preserved")
		}
		return request
	default:
		t.Fatal("successful deployment operation did not reach native peer")
		var zero T
		return zero
	}
}

func (server *deploymentCapabilityServer) DescribeWorkerDeployment(_ context.Context, request *workflowservice.DescribeWorkerDeploymentRequest) (*workflowservice.DescribeWorkerDeploymentResponse, error) {
	if err := server.record("DescribeWorkerDeployment", request); err != nil {
		return nil, err
	}
	return &workflowservice.DescribeWorkerDeploymentResponse{ConflictToken: []byte("current-token"), WorkerDeploymentInfo: &deploymentpb.WorkerDeploymentInfo{
		Name: request.DeploymentName, ManagerIdentity: "manager", LastModifierIdentity: "modifier",
		RoutingConfig:    &deploymentpb.RoutingConfig{CurrentDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: request.DeploymentName, BuildId: "current"}, RampingVersion: request.DeploymentName + ".ramping", RampingVersionPercentage: 17},
		VersionSummaries: []*deploymentpb.WorkerDeploymentInfo_WorkerDeploymentVersionSummary{{DeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: request.DeploymentName, BuildId: "current"}, DrainageStatus: enumspb.VERSION_DRAINAGE_STATUS_DRAINED}},
	}}, nil
}

func (server *deploymentCapabilityServer) SetWorkerDeploymentCurrentVersion(_ context.Context, request *workflowservice.SetWorkerDeploymentCurrentVersionRequest) (*workflowservice.SetWorkerDeploymentCurrentVersionResponse, error) {
	if err := server.record("SetWorkerDeploymentCurrentVersion", request); err != nil {
		return nil, err
	}
	return &workflowservice.SetWorkerDeploymentCurrentVersionResponse{ConflictToken: []byte("next-token"), PreviousDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{DeploymentName: request.DeploymentName, BuildId: "previous"}}, nil
}

func (server *deploymentCapabilityServer) SetWorkerDeploymentRampingVersion(_ context.Context, request *workflowservice.SetWorkerDeploymentRampingVersionRequest) (*workflowservice.SetWorkerDeploymentRampingVersionResponse, error) {
	if err := server.record("SetWorkerDeploymentRampingVersion", request); err != nil {
		return nil, err
	}
	return &workflowservice.SetWorkerDeploymentRampingVersionResponse{ConflictToken: []byte("ramp-token"), PreviousVersion: request.DeploymentName + ".previous", PreviousPercentage: 12}, nil
}

func (server *deploymentCapabilityServer) SetWorkerDeploymentManager(_ context.Context, request *workflowservice.SetWorkerDeploymentManagerRequest) (*workflowservice.SetWorkerDeploymentManagerResponse, error) {
	if err := server.record("SetWorkerDeploymentManager", request); err != nil {
		return nil, err
	}
	return &workflowservice.SetWorkerDeploymentManagerResponse{ConflictToken: []byte("manager-token"), PreviousManagerIdentity: "previous-manager"}, nil
}

func (server *deploymentCapabilityServer) DescribeWorkerDeploymentVersion(_ context.Context, request *workflowservice.DescribeWorkerDeploymentVersionRequest) (*workflowservice.DescribeWorkerDeploymentVersionResponse, error) {
	if err := server.record("DescribeWorkerDeploymentVersion", request); err != nil {
		return nil, err
	}
	return &workflowservice.DescribeWorkerDeploymentVersionResponse{WorkerDeploymentVersionInfo: &deploymentpb.WorkerDeploymentVersionInfo{
		DeploymentVersion: request.DeploymentVersion, RampPercentage: 17,
		DrainageInfo:   &deploymentpb.VersionDrainageInfo{Status: enumspb.VERSION_DRAINAGE_STATUS_DRAINED},
		TaskQueueInfos: []*deploymentpb.WorkerDeploymentVersionInfo_VersionTaskQueueInfo{{Name: "queue", Type: enumspb.TASK_QUEUE_TYPE_ACTIVITY}},
		Metadata:       &deploymentpb.VersionMetadata{Entries: map[string]*commonpb.Payload{"key": {Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: []byte(`"value"`)}}},
	}}, nil
}

func (server *deploymentCapabilityServer) DeleteWorkerDeploymentVersion(_ context.Context, request *workflowservice.DeleteWorkerDeploymentVersionRequest) (*workflowservice.DeleteWorkerDeploymentVersionResponse, error) {
	if err := server.record("DeleteWorkerDeploymentVersion", request); err != nil {
		return nil, err
	}
	return &workflowservice.DeleteWorkerDeploymentVersionResponse{}, nil
}

func (server *deploymentCapabilityServer) UpdateWorkerDeploymentVersionMetadata(_ context.Context, request *workflowservice.UpdateWorkerDeploymentVersionMetadataRequest) (*workflowservice.UpdateWorkerDeploymentVersionMetadataResponse, error) {
	if err := server.record("UpdateWorkerDeploymentVersionMetadata", request); err != nil {
		return nil, err
	}
	return &workflowservice.UpdateWorkerDeploymentVersionMetadataResponse{Metadata: &deploymentpb.VersionMetadata{Entries: request.UpsertEntries}}, nil
}

func (server *deploymentCapabilityServer) DeleteWorkerDeployment(_ context.Context, request *workflowservice.DeleteWorkerDeploymentRequest) (*workflowservice.DeleteWorkerDeploymentResponse, error) {
	if err := server.record("DeleteWorkerDeployment", request); err != nil {
		return nil, err
	}
	return &workflowservice.DeleteWorkerDeploymentResponse{}, nil
}

func (server *deploymentCapabilityServer) ListWorkerDeployments(_ context.Context, request *workflowservice.ListWorkerDeploymentsRequest) (*workflowservice.ListWorkerDeploymentsResponse, error) {
	if err := server.record("ListWorkerDeployments", request); err != nil {
		return nil, err
	}
	switch string(request.NextPageToken) {
	case "":
		return &workflowservice.ListWorkerDeploymentsResponse{NextPageToken: []byte("entry")}, nil
	case "entry":
		return &workflowservice.ListWorkerDeploymentsResponse{WorkerDeployments: []*workflowservice.ListWorkerDeploymentsResponse_WorkerDeploymentSummary{{Name: "deployment", RoutingConfig: &deploymentpb.RoutingConfig{CurrentVersion: "deployment.current"}}}, NextPageToken: []byte("end")}, nil
	case "end":
		return &workflowservice.ListWorkerDeploymentsResponse{}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "changed deployment continuation")
	}
}

func legacyCapabilityInfo() *deploymentpb.DeploymentInfo {
	return &deploymentpb.DeploymentInfo{Deployment: &deploymentpb.Deployment{SeriesName: "series", BuildId: "build"}, IsCurrent: true,
		TaskQueueInfos: []*deploymentpb.DeploymentInfo_TaskQueueInfo{{Name: "queue", Type: enumspb.TASK_QUEUE_TYPE_WORKFLOW, FirstPollerTime: timestamppb.New(time.Unix(123, 0))}}}
}

func (server *deploymentCapabilityServer) DescribeDeployment(_ context.Context, request *workflowservice.DescribeDeploymentRequest) (*workflowservice.DescribeDeploymentResponse, error) {
	if err := server.record("DescribeDeployment", request); err != nil {
		return nil, err
	}
	return &workflowservice.DescribeDeploymentResponse{DeploymentInfo: legacyCapabilityInfo()}, nil
}

func (server *deploymentCapabilityServer) GetDeploymentReachability(_ context.Context, request *workflowservice.GetDeploymentReachabilityRequest) (*workflowservice.GetDeploymentReachabilityResponse, error) {
	if err := server.record("GetDeploymentReachability", request); err != nil {
		return nil, err
	}
	return &workflowservice.GetDeploymentReachabilityResponse{DeploymentInfo: legacyCapabilityInfo(), Reachability: enumspb.DEPLOYMENT_REACHABILITY_REACHABLE, LastUpdateTime: timestamppb.New(time.Unix(456, 0))}, nil
}

func (server *deploymentCapabilityServer) GetCurrentDeployment(_ context.Context, request *workflowservice.GetCurrentDeploymentRequest) (*workflowservice.GetCurrentDeploymentResponse, error) {
	if err := server.record("GetCurrentDeployment", request); err != nil {
		return nil, err
	}
	return &workflowservice.GetCurrentDeploymentResponse{CurrentDeploymentInfo: legacyCapabilityInfo()}, nil
}

func (server *deploymentCapabilityServer) SetCurrentDeployment(_ context.Context, request *workflowservice.SetCurrentDeploymentRequest) (*workflowservice.SetCurrentDeploymentResponse, error) {
	if err := server.record("SetCurrentDeployment", request); err != nil {
		return nil, err
	}
	return &workflowservice.SetCurrentDeploymentResponse{CurrentDeploymentInfo: legacyCapabilityInfo(), PreviousDeploymentInfo: &deploymentpb.DeploymentInfo{Deployment: &deploymentpb.Deployment{SeriesName: "series", BuildId: "old"}}}, nil
}

func (server *deploymentCapabilityServer) ListDeployments(_ context.Context, request *workflowservice.ListDeploymentsRequest) (*workflowservice.ListDeploymentsResponse, error) {
	if err := server.record("ListDeployments", request); err != nil {
		return nil, err
	}
	switch string(request.NextPageToken) {
	case "":
		return &workflowservice.ListDeploymentsResponse{NextPageToken: []byte("entry")}, nil
	case "entry":
		return &workflowservice.ListDeploymentsResponse{Deployments: []*deploymentpb.DeploymentListInfo{{Deployment: legacyCapabilityInfo().Deployment, IsCurrent: true}}, NextPageToken: []byte("end")}, nil
	case "end":
		return &workflowservice.ListDeploymentsResponse{}, nil
	default:
		return nil, status.Error(codes.InvalidArgument, "changed legacy continuation")
	}
}

func (server *deploymentCapabilityServer) GetWorkerBuildIdCompatibility(_ context.Context, request *workflowservice.GetWorkerBuildIdCompatibilityRequest) (*workflowservice.GetWorkerBuildIdCompatibilityResponse, error) {
	if err := server.record("GetWorkerBuildIdCompatibility", request); err != nil {
		return nil, err
	}
	return &workflowservice.GetWorkerBuildIdCompatibilityResponse{MajorVersionSets: []*taskqueuepb.CompatibleVersionSet{{BuildIds: []string{"old", "default"}}}}, nil
}

func (server *deploymentCapabilityServer) GetWorkerTaskReachability(_ context.Context, request *workflowservice.GetWorkerTaskReachabilityRequest) (*workflowservice.GetWorkerTaskReachabilityResponse, error) {
	if err := server.record("GetWorkerTaskReachability", request); err != nil {
		return nil, err
	}
	return &workflowservice.GetWorkerTaskReachabilityResponse{BuildIdReachability: []*taskqueuepb.BuildIdReachability{{BuildId: "build", TaskQueueReachability: []*taskqueuepb.TaskQueueReachability{
		{TaskQueue: "queue", Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS}},
		{TaskQueue: "unretrieved", Reachability: []enumspb.TaskReachability{enumspb.TASK_REACHABILITY_UNSPECIFIED}},
	}}}}, nil
}

func (server *deploymentCapabilityServer) GetWorkerVersioningRules(_ context.Context, request *workflowservice.GetWorkerVersioningRulesRequest) (*workflowservice.GetWorkerVersioningRulesResponse, error) {
	if err := server.record("GetWorkerVersioningRules", request); err != nil {
		return nil, err
	}
	return &workflowservice.GetWorkerVersioningRulesResponse{ConflictToken: []byte("rule-token"),
		AssignmentRules:         []*taskqueuepb.TimestampedBuildIdAssignmentRule{{Rule: &taskqueuepb.BuildIdAssignmentRule{TargetBuildId: "build"}, CreateTime: timestamppb.New(time.Unix(123, 0))}},
		CompatibleRedirectRules: []*taskqueuepb.TimestampedCompatibleBuildIdRedirectRule{{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "old", TargetBuildId: "build"}}},
	}, nil
}

func (server *deploymentCapabilityServer) UpdateWorkerBuildIdCompatibility(_ context.Context, request *workflowservice.UpdateWorkerBuildIdCompatibilityRequest) (*workflowservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	if err := server.record("UpdateWorkerBuildIdCompatibility", request); err != nil {
		return nil, err
	}
	return &workflowservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil
}

func (server *deploymentCapabilityServer) UpdateWorkerVersioningRules(_ context.Context, request *workflowservice.UpdateWorkerVersioningRulesRequest) (*workflowservice.UpdateWorkerVersioningRulesResponse, error) {
	if err := server.record("UpdateWorkerVersioningRules", request); err != nil {
		return nil, err
	}
	return &workflowservice.UpdateWorkerVersioningRulesResponse{ConflictToken: []byte("next-rule-token"), AssignmentRules: []*taskqueuepb.TimestampedBuildIdAssignmentRule{{Rule: request.GetInsertAssignmentRule().GetRule()}}}, nil
}

func TestPublicWorkerDeploymentNativeOptionsConversionsAndDefaults(t *testing.T) {
	peer := newDeploymentCapabilityServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{}, deploymentCapabilityGrants...)
	client := fixture.owner.Client()
	handle, err := client.GetWorkerDeployment("deployment")
	if err != nil || len(peer.requests) != 0 {
		t.Fatal("deployment handle construction was not local", err)
	}
	described, err := handle.Describe(fixture.ctx, sdk.WorkerDeploymentDescribeOptions{})
	if err != nil || string(described.ConflictToken) != "current-token" || described.Info.Name != "deployment" || !described.Info.CreateTime.IsZero() ||
		described.Info.ManagerIdentity != "manager" || described.Info.LastModifierIdentity != "modifier" ||
		described.Info.RoutingConfig.CurrentVersion.BuildID != "current" || described.Info.RoutingConfig.RampingVersion.BuildID != "ramping" ||
		described.Info.RoutingConfig.RampingVersionPercentage != 17 || described.Info.VersionSummaries[0].DrainageStatus != sdk.WorkerDeploymentVersionDrainageStatusDrained {
		t.Fatal("native deployment description conversion changed", err)
	}
	if request := deploymentRequest[*workflowservice.DescribeWorkerDeploymentRequest](t, peer); request.DeploymentName != "deployment" {
		t.Fatal("Describe lost native target")
	}
	if result := capabilityEvidence(t, fixture, "deployment.describeworkerdeployment"); !result.Execution.ResultObtained || result.Source != client.Attribution() || result.Execution.DeploymentName != "deployment" {
		t.Fatal("Describe lost observation or exact use")
	}
	current, err := handle.SetCurrentVersion(fixture.ctx, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "next", ConflictToken: described.ConflictToken, Identity: "operator", IgnoreMissingTaskQueues: true, AllowNoPollers: true})
	if err != nil || string(current.ConflictToken) != "next-token" || current.PreviousVersion.BuildID != "previous" {
		t.Fatal("SetCurrentVersion response conversion changed", err)
	}
	request := deploymentRequest[*workflowservice.SetWorkerDeploymentCurrentVersionRequest](t, peer)
	if request.DeploymentName != "deployment" || request.BuildId != "next" || request.Version != "deployment.next" || string(request.ConflictToken) != "current-token" || request.Identity != "operator" || !request.IgnoreMissingTaskQueues || !request.AllowNoPollers {
		t.Fatal("SetCurrentVersion options or canonical compatibility version changed")
	}
	if !capabilityEvidence(t, fixture, "deployment.setworkerdeploymentcurrentversion").Execution.Accepted {
		t.Fatal("current version acknowledgement lost")
	}
	if _, err := handle.SetCurrentVersion(fixture.ctx, sdk.WorkerDeploymentSetCurrentVersionOptions{}); err != nil {
		t.Fatal("native unversioned/default request was rejected", err)
	}
	request = deploymentRequest[*workflowservice.SetWorkerDeploymentCurrentVersionRequest](t, peer)
	if request.Version != "__unversioned__" || request.BuildId != "" || request.Identity != "fathomry" || len(request.ConflictToken) != 0 || request.IgnoreMissingTaskQueues || request.AllowNoPollers {
		t.Fatal("zero version options changed native defaults")
	}
	capabilityEvidence(t, fixture, "deployment.setworkerdeploymentcurrentversion")
	ramp, err := handle.SetRampingVersion(fixture.ctx, sdk.WorkerDeploymentSetRampingVersionOptions{BuildID: "candidate", Percentage: 23, ConflictToken: current.ConflictToken, Identity: "operator", IgnoreMissingTaskQueues: true, AllowNoPollers: true})
	if err != nil || string(ramp.ConflictToken) != "ramp-token" || ramp.PreviousVersion.BuildID != "previous" || ramp.PreviousPercentage != 12 {
		t.Fatal("ramping response or fallback version conversion changed", err)
	}
	rampRequest := deploymentRequest[*workflowservice.SetWorkerDeploymentRampingVersionRequest](t, peer)
	if rampRequest.DeploymentName != "deployment" || rampRequest.BuildId != "candidate" || rampRequest.Version != "deployment.candidate" || rampRequest.Percentage != 23 || string(rampRequest.ConflictToken) != "next-token" || rampRequest.Identity != "operator" || !rampRequest.IgnoreMissingTaskQueues || !rampRequest.AllowNoPollers {
		t.Fatal("ramping options changed")
	}
	capabilityEvidence(t, fixture, "deployment.setworkerdeploymentrampingversion")
	manager, err := handle.SetManagerIdentity(fixture.ctx, sdk.WorkerDeploymentSetManagerIdentityOptions{Self: true, ConflictToken: ramp.ConflictToken})
	if err != nil || manager.PreviousManagerIdentity != "previous-manager" || string(manager.ConflictToken) != "manager-token" {
		t.Fatal("manager response conversion changed", err)
	}
	managerRequest := deploymentRequest[*workflowservice.SetWorkerDeploymentManagerRequest](t, peer)
	if !managerRequest.GetSelf() || managerRequest.Identity != "fathomry" || managerRequest.DeploymentName != "deployment" || string(managerRequest.ConflictToken) != "ramp-token" {
		t.Fatal("manager oneof or default identity changed")
	}
	capabilityEvidence(t, fixture, "deployment.setworkerdeploymentmanager")
	version, err := handle.DescribeVersion(fixture.ctx, sdk.WorkerDeploymentDescribeVersionOptions{BuildID: "build"})
	if err != nil || version.Info.Version != (worker.WorkerDeploymentVersion{DeploymentName: "deployment", BuildID: "build"}) || !version.Info.CreateTime.IsZero() ||
		version.Info.RampPercentage != 17 || version.Info.DrainageInfo.DrainageStatus != sdk.WorkerDeploymentVersionDrainageStatusDrained ||
		version.Info.TaskQueuesInfos[0].Name != "queue" || version.Info.TaskQueuesInfos[0].Type != sdk.TaskQueueTypeActivity {
		t.Fatal("deployment version/drainage conversion changed", err)
	}
	versionRequest := deploymentRequest[*workflowservice.DescribeWorkerDeploymentVersionRequest](t, peer)
	if versionRequest.Version != "deployment.build" || versionRequest.DeploymentVersion.BuildId != "build" || versionRequest.DeploymentVersion.DeploymentName != "deployment" {
		t.Fatal("DescribeVersion target conversion changed")
	}
	capabilityEvidence(t, fixture, "deployment.describeworkerdeploymentversion")
	metadata, err := handle.UpdateVersionMetadata(fixture.ctx, sdk.WorkerDeploymentUpdateVersionMetadataOptions{Version: worker.WorkerDeploymentVersion{DeploymentName: "other", BuildID: "build"},
		MetadataUpdate: sdk.WorkerDeploymentMetadataUpdate{UpsertEntries: map[string]any{"encoded": version.Info.Metadata["key"], "plain": "second"}, RemoveEntries: []string{"removed"}}})
	if err != nil {
		t.Fatal(err)
	}
	metadataRequest := deploymentRequest[*workflowservice.UpdateWorkerDeploymentVersionMetadataRequest](t, peer)
	var plain string
	if metadataRequest.Version != "other.build" || metadataRequest.DeploymentVersion.DeploymentName != "other" || metadataRequest.DeploymentVersion.BuildId != "build" ||
		!reflect.DeepEqual(metadataRequest.RemoveEntries, []string{"removed"}) || !proto.Equal(metadataRequest.UpsertEntries["encoded"], version.Info.Metadata["key"]) ||
		converter.GetDefaultDataConverter().FromPayload(metadata.Metadata["plain"], &plain) != nil || plain != "second" {
		t.Fatal("metadata target override, pre-encoded value or native data conversion changed")
	}
	if result := capabilityEvidence(t, fixture, "deployment.metadata"); result.Execution.DeploymentName != "other" || !result.Execution.Accepted {
		t.Fatal("metadata evidence used handle name instead of actual target")
	}
	if _, err := handle.DeleteVersion(fixture.ctx, sdk.WorkerDeploymentDeleteVersionOptions{BuildID: "build", SkipDrainage: true, Identity: "operator"}); err != nil {
		t.Fatal(err)
	}
	deleted := deploymentRequest[*workflowservice.DeleteWorkerDeploymentVersionRequest](t, peer)
	if deleted.Version != "deployment.build" || deleted.DeploymentVersion.BuildId != "build" || deleted.DeploymentVersion.DeploymentName != "deployment" || !deleted.SkipDrainage || deleted.Identity != "operator" {
		t.Fatal("DeleteVersion options changed")
	}
	capabilityEvidence(t, fixture, "deployment.deleteworkerdeploymentversion")
	if _, err := client.DeleteWorkerDeployment(fixture.ctx, sdk.WorkerDeploymentDeleteOptions{Name: "deployment", Identity: "operator"}); err != nil {
		t.Fatal(err)
	}
	if deleted := deploymentRequest[*workflowservice.DeleteWorkerDeploymentRequest](t, peer); deleted.DeploymentName != "deployment" || deleted.Identity != "operator" {
		t.Fatal("DeleteWorkerDeployment target changed")
	}
	capabilityEvidence(t, fixture, "deployment.delete")
}

func TestPublicLegacyDeploymentNativeFamily(t *testing.T) {
	peer := newDeploymentCapabilityServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{}, deploymentCapabilityGrants...)
	origin := fixture.owner.Client()
	client := origin.DeploymentClient()
	if len(peer.requests) != 0 {
		t.Fatal("legacy facade construction was not local")
	}
	id := sdk.Deployment{SeriesName: "series", BuildID: "build"}
	described, err := client.Describe(fixture.ctx, sdk.DeploymentDescribeOptions{Deployment: id})
	if err != nil || described.DeploymentInfo.Deployment != id || !described.DeploymentInfo.IsCurrent ||
		described.DeploymentInfo.TaskQueuesInfos[0].Name != "queue" || !described.DeploymentInfo.TaskQueuesInfos[0].FirstPollerTime.Equal(time.Unix(123, 0)) {
		t.Fatal("legacy native description conversion changed", err)
	}
	if request := deploymentRequest[*workflowservice.DescribeDeploymentRequest](t, peer); request.Deployment.SeriesName != "series" || request.Deployment.BuildId != "build" {
		t.Fatal("legacy Describe target changed")
	}
	if result := capabilityEvidence(t, fixture, "legacy-deployment.describedeployment"); result.Source != origin.Attribution() || !result.Execution.ResultObtained {
		t.Fatal("legacy describe lost original use")
	}
	reachable, err := client.GetReachability(fixture.ctx, sdk.DeploymentGetReachabilityOptions{Deployment: id})
	if err != nil || reachable.DeploymentInfo.Deployment != id || reachable.Reachability != sdk.DeploymentReachabilityReachable || !reachable.LastUpdateTime.Equal(time.Unix(456, 0)) {
		t.Fatal("legacy reachability conversion changed", err)
	}
	if request := deploymentRequest[*workflowservice.GetDeploymentReachabilityRequest](t, peer); request.Deployment.SeriesName != "series" || request.Deployment.BuildId != "build" {
		t.Fatal("legacy reachability target changed")
	}
	capabilityEvidence(t, fixture, "legacy-deployment.getdeploymentreachability")
	current, err := client.GetCurrent(fixture.ctx, sdk.DeploymentGetCurrentOptions{SeriesName: "series"})
	if err != nil || current.DeploymentInfo.Deployment != id {
		t.Fatal("legacy GetCurrent conversion changed", err)
	}
	if request := deploymentRequest[*workflowservice.GetCurrentDeploymentRequest](t, peer); request.SeriesName != "series" {
		t.Fatal("legacy GetCurrent series changed")
	}
	capabilityEvidence(t, fixture, "legacy-deployment.getcurrentdeployment")
	updated, err := client.SetCurrent(fixture.ctx, sdk.DeploymentSetCurrentOptions{Deployment: id, MetadataUpdate: sdk.DeploymentMetadataUpdate{UpsertEntries: map[string]any{"key": "value"}, RemoveEntries: []string{"removed"}}})
	if err != nil || updated.Current.Deployment != id || updated.Previous.Deployment.BuildID != "old" {
		t.Fatal("legacy SetCurrent previous/current conversion changed", err)
	}
	request := deploymentRequest[*workflowservice.SetCurrentDeploymentRequest](t, peer)
	var value string
	if request.Identity != "fathomry" || request.Deployment.SeriesName != "series" || request.Deployment.BuildId != "build" ||
		!reflect.DeepEqual(request.UpdateMetadata.RemoveEntries, []string{"removed"}) || converter.GetDefaultDataConverter().FromPayload(request.UpdateMetadata.UpsertEntries["key"], &value) != nil || value != "value" {
		t.Fatal("legacy metadata/native client identity changed")
	}
	if !capabilityEvidence(t, fixture, "legacy-deployment.setcurrentdeployment").Execution.Accepted {
		t.Fatal("legacy mutation acknowledgement lost")
	}
}

func TestPublicDeploymentWalksEmptyPagesAndVisitorFailure(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%t", legacy), func(t *testing.T) {
			peer := newDeploymentCapabilityServer()
			fixture := newCapabilityFixture(t, peer, NativeOptions{}, "ListWorkerDeployments", "ListDeployments")
			client := fixture.owner.Client()
			operation := "deployment.list"
			if legacy {
				operation = "legacy-deployment.listdeployments"
			}
			for _, stopEarly := range []bool{false, true} {
				stopped := errors.New("private deployment visitor canary")
				visited := 0
				var err error
				if legacy {
					err = client.DeploymentClient().Walk(fixture.ctx, sdk.DeploymentListOptions{PageSize: 1, SeriesName: "series"}, func(ctx context.Context, entry *sdk.DeploymentListEntry) error {
						visited++
						if ctx.Err() != nil || entry.Deployment != (sdk.Deployment{SeriesName: "series", BuildID: "build"}) || !entry.IsCurrent {
							t.Error("legacy entry conversion or live visitor context changed")
						}
						if stopEarly {
							return stopped
						}
						return nil
					})
				} else {
					err = client.WalkWorkerDeployments(fixture.ctx, sdk.WorkerDeploymentListOptions{PageSize: 1}, func(ctx context.Context, entry *sdk.WorkerDeploymentListEntry) error {
						visited++
						if ctx.Err() != nil || entry.Name != "deployment" || entry.RoutingConfig.CurrentVersion.BuildID != "current" || !entry.CreateTime.IsZero() {
							t.Error("current entry conversion or zero timestamp changed")
						}
						if stopEarly {
							return stopped
						}
						return nil
					})
				}
				wantTokens := []string{"", "entry", "end"}
				if stopEarly {
					wantTokens = wantTokens[:2]
					if !errors.Is(err, stopped) || strings.Contains(err.Error(), "canary") {
						t.Fatal("native visitor error was erased or leaked")
					}
				} else if err != nil {
					t.Fatal(err)
				}
				if visited != 1 || len(peer.requests) != len(wantTokens) {
					t.Fatal("empty page ended walk or failure fetched another page")
				}
				for _, token := range wantTokens {
					if legacy {
						request := deploymentRequest[*workflowservice.ListDeploymentsRequest](t, peer)
						if request.PageSize != 1 || request.SeriesName != "series" || string(request.NextPageToken) != token {
							t.Fatal("legacy list options/continuation changed")
						}
					} else {
						request := deploymentRequest[*workflowservice.ListWorkerDeploymentsRequest](t, peer)
						if request.PageSize != 1 || string(request.NextPageToken) != token {
							t.Fatal("current list options/continuation changed")
						}
					}
				}
				result := capabilityEvidence(t, fixture, operation)
				if result.Source != client.Attribution() || result.Execution.ResultObtained == stopEarly {
					t.Fatal("walk evidence confused exhausted and failed enumeration")
				}
				if cause, present := result.NativeError(); stopEarly && (!present || cause != stopped) || !stopEarly && present {
					t.Fatal("independent walk error did not match exact visitor return")
				}
			}
		})
	}
}

func TestPublicWorkerVersioningDirectCounterparts(t *testing.T) {
	peer := newDeploymentCapabilityServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{}, deploymentCapabilityGrants...)
	client := fixture.owner.Client()
	sets, err := client.GetWorkerBuildIdCompatibility(fixture.ctx, &sdk.GetWorkerBuildIdCompatibilityOptions{TaskQueue: "queue", MaxSets: 2})
	if err != nil || sets.Default() != "default" || !reflect.DeepEqual(sets.Sets[0].BuildIDs, []string{"old", "default"}) {
		t.Fatal("native version-set conversion changed", err)
	}
	if request := deploymentRequest[*workflowservice.GetWorkerBuildIdCompatibilityRequest](t, peer); request.TaskQueue != "queue" || request.MaxSets != 2 {
		t.Fatal("build compatibility options changed")
	}
	capabilityEvidence(t, fixture, "worker.build-compatibility")
	reachable, err := client.GetWorkerTaskReachability(fixture.ctx, &sdk.GetWorkerTaskReachabilityOptions{BuildIDs: []string{"build"}, TaskQueues: []string{"queue", "unretrieved"}, Reachability: sdk.TaskReachabilityOpenWorkflows})
	if err != nil || !reflect.DeepEqual(reachable.BuildIDReachability["build"].UnretrievedTaskQueues, []string{"unretrieved"}) || reachable.BuildIDReachability["build"].TaskQueueReachable["queue"].TaskQueueReachability[0] != sdk.TaskReachabilityOpenWorkflows {
		t.Fatal("native retrieved versus unretrieved reachability changed", err)
	}
	reachRequest := deploymentRequest[*workflowservice.GetWorkerTaskReachabilityRequest](t, peer)
	if !reflect.DeepEqual(reachRequest.BuildIds, []string{"build"}) || !reflect.DeepEqual(reachRequest.TaskQueues, []string{"queue", "unretrieved"}) || reachRequest.Reachability != enumspb.TASK_REACHABILITY_OPEN_WORKFLOWS {
		t.Fatal("reachability request conversion changed")
	}
	capabilityEvidence(t, fixture, "worker.task-reachability")
	rules, err := client.GetWorkerVersioningRules(fixture.ctx, sdk.GetWorkerVersioningOptions{TaskQueue: "queue"})
	if err != nil || rules.AssignmentRules[0].Rule.TargetBuildID != "build" || !rules.AssignmentRules[0].CreateTime.Equal(time.Unix(123, 0)) || rules.RedirectRules[0].Rule.SourceBuildID != "old" || rules.RedirectRules[0].Rule.TargetBuildID != "build" {
		t.Fatal("native assignment/redirect conversion changed", err)
	}
	if request := deploymentRequest[*workflowservice.GetWorkerVersioningRulesRequest](t, peer); request.TaskQueue != "queue" {
		t.Fatal("versioning request target changed")
	}
	capabilityEvidence(t, fixture, "worker.versioning-rules")
	if err := client.UpdateWorkerBuildIdCompatibility(fixture.ctx, &sdk.UpdateWorkerBuildIdCompatibilityOptions{TaskQueue: "queue", Operation: &sdk.BuildIDOpAddNewCompatibleVersion{BuildID: "build", ExistingCompatibleBuildID: "old", MakeSetDefault: true}}); err != nil {
		t.Fatal(err)
	}
	buildRequest := deploymentRequest[*workflowservice.UpdateWorkerBuildIdCompatibilityRequest](t, peer)
	if buildRequest.TaskQueue != "queue" || buildRequest.GetAddNewCompatibleBuildId().GetNewBuildId() != "build" || buildRequest.GetAddNewCompatibleBuildId().GetExistingCompatibleBuildId() != "old" || !buildRequest.GetAddNewCompatibleBuildId().GetMakeSetDefault() {
		t.Fatal("native build operation oneof conversion changed")
	}
	if !capabilityEvidence(t, fixture, "worker.update-build-compatibility").Execution.Accepted {
		t.Fatal("build update lost acknowledgement")
	}
	updated, err := client.UpdateWorkerVersioningRules(fixture.ctx, sdk.UpdateWorkerVersioningRulesOptions{TaskQueue: "queue", ConflictToken: rules.ConflictToken,
		Operation: &sdk.VersioningOperationInsertAssignmentRule{RuleIndex: 1, Rule: sdk.VersioningAssignmentRule{TargetBuildID: "next", Ramp: &sdk.VersioningRampByPercentage{Percentage: 25}}}})
	if err != nil || updated.AssignmentRules[0].Rule.TargetBuildID != "next" {
		t.Fatal("native versioning update response changed", err)
	}
	rulesRequest := deploymentRequest[*workflowservice.UpdateWorkerVersioningRulesRequest](t, peer)
	if rulesRequest.TaskQueue != "queue" || string(rulesRequest.ConflictToken) != "rule-token" || rulesRequest.GetInsertAssignmentRule().GetRuleIndex() != 1 || rulesRequest.GetInsertAssignmentRule().GetRule().GetTargetBuildId() != "next" || rulesRequest.GetInsertAssignmentRule().GetRule().GetPercentageRamp().GetRampPercentage() != 25 {
		t.Fatal("native conflict token, assignment position or ramp changed")
	}
	capabilityEvidence(t, fixture, "worker.update-versioning-rules")
}

type deploymentCapabilityCall struct {
	method    string
	operation string
	invoke    func(context.Context) error
}

func deploymentCapabilityCalls(client *Client, handle *WorkerDeployment, legacy *DeploymentClient) []deploymentCapabilityCall {
	version := worker.WorkerDeploymentVersion{DeploymentName: "deployment", BuildID: "build"}
	id := sdk.Deployment{SeriesName: "series", BuildID: "build"}
	return []deploymentCapabilityCall{
		{"DescribeWorkerDeployment", "deployment.describeworkerdeployment", func(ctx context.Context) error {
			_, err := handle.Describe(ctx, sdk.WorkerDeploymentDescribeOptions{})
			return err
		}},
		{"SetWorkerDeploymentCurrentVersion", "deployment.setworkerdeploymentcurrentversion", func(ctx context.Context) error {
			_, err := handle.SetCurrentVersion(ctx, sdk.WorkerDeploymentSetCurrentVersionOptions{BuildID: "build"})
			return err
		}},
		{"SetWorkerDeploymentRampingVersion", "deployment.setworkerdeploymentrampingversion", func(ctx context.Context) error {
			_, err := handle.SetRampingVersion(ctx, sdk.WorkerDeploymentSetRampingVersionOptions{BuildID: "build", Percentage: 10})
			return err
		}},
		{"SetWorkerDeploymentManager", "deployment.setworkerdeploymentmanager", func(ctx context.Context) error {
			_, err := handle.SetManagerIdentity(ctx, sdk.WorkerDeploymentSetManagerIdentityOptions{Self: true})
			return err
		}},
		{"DescribeWorkerDeploymentVersion", "deployment.describeworkerdeploymentversion", func(ctx context.Context) error {
			_, err := handle.DescribeVersion(ctx, sdk.WorkerDeploymentDescribeVersionOptions{BuildID: "build"})
			return err
		}},
		{"DeleteWorkerDeploymentVersion", "deployment.deleteworkerdeploymentversion", func(ctx context.Context) error {
			_, err := handle.DeleteVersion(ctx, sdk.WorkerDeploymentDeleteVersionOptions{BuildID: "build"})
			return err
		}},
		{"UpdateWorkerDeploymentVersionMetadata", "deployment.metadata", func(ctx context.Context) error {
			_, err := handle.UpdateVersionMetadata(ctx, sdk.WorkerDeploymentUpdateVersionMetadataOptions{Version: version})
			return err
		}},
		{"DeleteWorkerDeployment", "deployment.delete", func(ctx context.Context) error {
			_, err := client.DeleteWorkerDeployment(ctx, sdk.WorkerDeploymentDeleteOptions{Name: "deployment"})
			return err
		}},
		{"ListWorkerDeployments", "deployment.list", func(ctx context.Context) error {
			return client.WalkWorkerDeployments(ctx, sdk.WorkerDeploymentListOptions{}, func(context.Context, *sdk.WorkerDeploymentListEntry) error { return nil })
		}},
		{"DescribeDeployment", "legacy-deployment.describedeployment", func(ctx context.Context) error {
			_, err := legacy.Describe(ctx, sdk.DeploymentDescribeOptions{Deployment: id})
			return err
		}},
		{"GetDeploymentReachability", "legacy-deployment.getdeploymentreachability", func(ctx context.Context) error {
			_, err := legacy.GetReachability(ctx, sdk.DeploymentGetReachabilityOptions{Deployment: id})
			return err
		}},
		{"GetCurrentDeployment", "legacy-deployment.getcurrentdeployment", func(ctx context.Context) error {
			_, err := legacy.GetCurrent(ctx, sdk.DeploymentGetCurrentOptions{SeriesName: "series"})
			return err
		}},
		{"SetCurrentDeployment", "legacy-deployment.setcurrentdeployment", func(ctx context.Context) error {
			_, err := legacy.SetCurrent(ctx, sdk.DeploymentSetCurrentOptions{Deployment: id})
			return err
		}},
		{"ListDeployments", "legacy-deployment.listdeployments", func(ctx context.Context) error {
			return legacy.Walk(ctx, sdk.DeploymentListOptions{}, func(context.Context, *sdk.DeploymentListEntry) error { return nil })
		}},
		{"GetWorkerBuildIdCompatibility", "worker.build-compatibility", func(ctx context.Context) error {
			_, err := client.GetWorkerBuildIdCompatibility(ctx, &sdk.GetWorkerBuildIdCompatibilityOptions{TaskQueue: "queue"})
			return err
		}},
		{"GetWorkerTaskReachability", "worker.task-reachability", func(ctx context.Context) error {
			_, err := client.GetWorkerTaskReachability(ctx, &sdk.GetWorkerTaskReachabilityOptions{BuildIDs: []string{"build"}})
			return err
		}},
		{"GetWorkerVersioningRules", "worker.versioning-rules", func(ctx context.Context) error {
			_, err := client.GetWorkerVersioningRules(ctx, sdk.GetWorkerVersioningOptions{TaskQueue: "queue"})
			return err
		}},
		{"UpdateWorkerBuildIdCompatibility", "worker.update-build-compatibility", func(ctx context.Context) error {
			return client.UpdateWorkerBuildIdCompatibility(ctx, &sdk.UpdateWorkerBuildIdCompatibilityOptions{TaskQueue: "queue", Operation: &sdk.BuildIDOpAddNewIDInNewDefaultSet{BuildID: "build"}})
		}},
		{"UpdateWorkerVersioningRules", "worker.update-versioning-rules", func(ctx context.Context) error {
			_, err := client.UpdateWorkerVersioningRules(ctx, sdk.UpdateWorkerVersioningRulesOptions{TaskQueue: "queue", Operation: &sdk.VersioningOperationInsertAssignmentRule{Rule: sdk.VersioningAssignmentRule{TargetBuildID: "build"}}})
			return err
		}},
	}
}

func TestPublicDeploymentExactGrantsAndClosedOrigin(t *testing.T) {
	for _, mode := range []string{"no-grants", "read-grant", "closed-origin"} {
		t.Run(mode, func(t *testing.T) {
			peer := newDeploymentCapabilityServer()
			var grants []string
			if mode == "read-grant" {
				grants = []string{"DescribeWorkerDeployment"}
			} else if mode == "closed-origin" {
				grants = deploymentCapabilityGrants
			}
			fixture := newCapabilityFixture(t, peer, NativeOptions{}, grants...)
			client := fixture.owner.Client()
			handle, err := client.GetWorkerDeployment("deployment")
			if err != nil {
				t.Fatal(err)
			}
			legacy := client.DeploymentClient()
			var other *Client
			want := ErrAuthority
			if mode == "closed-origin" {
				other, err = client.Borrow(fixture.ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Close(fixture.ctx); err != nil {
					t.Fatal(err)
				}
				want = ErrState
			}
			for _, call := range deploymentCapabilityCalls(client, handle, legacy) {
				err := call.invoke(fixture.ctx)
				if mode == "read-grant" && call.method == "DescribeWorkerDeployment" {
					if err != nil {
						t.Fatal("exact read grant was not usable", err)
					}
					deploymentRequest[*workflowservice.DescribeWorkerDeploymentRequest](t, peer)
					capabilityEvidence(t, fixture, call.operation)
				} else if !errors.Is(err, want) || len(peer.requests) != 0 {
					t.Fatal(call.method, "crossed exact grant or retained origin boundary", err)
				}
			}
			if other != nil {
				peerHandle, err := other.GetWorkerDeployment("deployment")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := peerHandle.Describe(fixture.ctx, sdk.WorkerDeploymentDescribeOptions{}); err != nil {
					t.Fatal("closed origin revoked independent Borrow", err)
				}
				deploymentRequest[*workflowservice.DescribeWorkerDeploymentRequest](t, peer)
				if result := capabilityEvidence(t, fixture, "deployment.describeworkerdeployment"); result.Source != other.Attribution() {
					t.Fatal("peer handle evidence used closed origin")
				}
			}
		})
	}
}

func TestPublicDeploymentNativeFailuresRemainSafeAndExact(t *testing.T) {
	for _, method := range []string{"DescribeWorkerDeployment", "SetWorkerDeploymentCurrentVersion", "DescribeDeployment", "UpdateWorkerVersioningRules"} {
		t.Run(method, func(t *testing.T) {
			peer := newDeploymentCapabilityServer()
			peer.failMethod, peer.failure = method, status.Error(codes.FailedPrecondition, "private deployment conflict canary")
			fixture := newCapabilityFixture(t, peer, NativeOptions{}, deploymentCapabilityGrants...)
			client := fixture.owner.Client()
			handle, err := client.GetWorkerDeployment("deployment")
			if err != nil {
				t.Fatal(err)
			}
			for _, call := range deploymentCapabilityCalls(client, handle, client.DeploymentClient()) {
				if call.method != method {
					continue
				}
				err := call.invoke(fixture.ctx)
				semantic, present := NativeError(err)
				conflict, exact := semantic.(*serviceerror.FailedPrecondition)
				if !errors.Is(err, ErrExecution) || !present || !exact || !strings.Contains(conflict.Error(), "canary") || strings.Contains(fmt.Sprintf("%+v", err), "canary") || len(peer.requests) != 1 {
					t.Fatal("public deployment failure lost exact native return or safe presentation")
				}
				result := capabilityEvidence(t, fixture, call.operation)
				semantic, present = result.NativeError()
				if _, exact := semantic.(*serviceerror.FailedPrecondition); !present || !exact || result.Source != client.Attribution() || !result.Execution.NativeCalled || result.Execution.Accepted || result.Execution.ResultObtained {
					t.Fatal("independent deployment failure evidence changed")
				}
			}
		})
	}
}

func TestPublicDeploymentNativeValidationAndZeroHandles(t *testing.T) {
	peer := newDeploymentCapabilityServer()
	fixture := newCapabilityFixture(t, peer, NativeOptions{}, deploymentCapabilityGrants...)
	client := fixture.owner.Client()
	handle, err := client.GetWorkerDeployment("deployment")
	if err != nil {
		t.Fatal(err)
	}
	legacy := client.DeploymentClient()
	for _, call := range []deploymentCapabilityCall{
		{"", "deployment.describeworkerdeploymentversion", func(ctx context.Context) error {
			_, err := handle.DescribeVersion(ctx, sdk.WorkerDeploymentDescribeVersionOptions{})
			return err
		}},
		{"", "deployment.deleteworkerdeploymentversion", func(ctx context.Context) error {
			_, err := handle.DeleteVersion(ctx, sdk.WorkerDeploymentDeleteVersionOptions{})
			return err
		}},
		{"", "deployment.metadata", func(ctx context.Context) error {
			_, err := handle.UpdateVersionMetadata(ctx, sdk.WorkerDeploymentUpdateVersionMetadataOptions{})
			return err
		}},
		{"", "deployment.setworkerdeploymentmanager", func(ctx context.Context) error {
			_, err := handle.SetManagerIdentity(ctx, sdk.WorkerDeploymentSetManagerIdentityOptions{Self: true, ManagerIdentity: "ambiguous"})
			return err
		}},
		{"", "deployment.delete", func(ctx context.Context) error {
			_, err := client.DeleteWorkerDeployment(ctx, sdk.WorkerDeploymentDeleteOptions{})
			return err
		}},
		{"", "legacy-deployment.describedeployment", func(ctx context.Context) error {
			_, err := legacy.Describe(ctx, sdk.DeploymentDescribeOptions{})
			return err
		}},
		{"", "legacy-deployment.getdeploymentreachability", func(ctx context.Context) error {
			_, err := legacy.GetReachability(ctx, sdk.DeploymentGetReachabilityOptions{})
			return err
		}},
		{"", "legacy-deployment.getcurrentdeployment", func(ctx context.Context) error {
			_, err := legacy.GetCurrent(ctx, sdk.DeploymentGetCurrentOptions{})
			return err
		}},
		{"", "legacy-deployment.setcurrentdeployment", func(ctx context.Context) error {
			_, err := legacy.SetCurrent(ctx, sdk.DeploymentSetCurrentOptions{})
			return err
		}},
		{"", "worker.build-compatibility", func(ctx context.Context) error {
			_, err := client.GetWorkerBuildIdCompatibility(ctx, &sdk.GetWorkerBuildIdCompatibilityOptions{MaxSets: -1})
			return err
		}},
		{"", "worker.versioning-rules", func(ctx context.Context) error {
			_, err := client.GetWorkerVersioningRules(ctx, sdk.GetWorkerVersioningOptions{})
			return err
		}},
		{"", "worker.update-build-compatibility", func(ctx context.Context) error {
			return client.UpdateWorkerBuildIdCompatibility(ctx, &sdk.UpdateWorkerBuildIdCompatibilityOptions{})
		}},
		{"", "worker.update-versioning-rules", func(ctx context.Context) error {
			_, err := client.UpdateWorkerVersioningRules(ctx, sdk.UpdateWorkerVersioningRulesOptions{})
			return err
		}},
	} {
		if err := call.invoke(fixture.ctx); err == nil || len(peer.requests) != 0 {
			t.Fatal(call.operation, "native validation was bypassed", err)
		}
		if result := capabilityEvidence(t, fixture, call.operation); result.Execution.Accepted || result.Execution.ResultObtained {
			t.Fatal("invalid native input claimed an effect or result")
		}
	}
	if _, err := client.GetWorkerDeployment(""); !errors.Is(err, ErrInput) {
		t.Fatal("empty deployment handle accepted", err)
	}
	for _, invoke := range []func() error{
		func() error { _, err := client.GetWorkerBuildIdCompatibility(fixture.ctx, nil); return err },
		func() error { _, err := client.GetWorkerTaskReachability(fixture.ctx, nil); return err },
		func() error { return client.UpdateWorkerBuildIdCompatibility(fixture.ctx, nil) },
		func() error { return client.WalkWorkerDeployments(fixture.ctx, sdk.WorkerDeploymentListOptions{}, nil) },
		func() error { return legacy.Walk(fixture.ctx, sdk.DeploymentListOptions{}, nil) },
	} {
		if err := invoke(); !errors.Is(err, ErrInput) || len(peer.requests) != 0 {
			t.Fatal("nil option/visitor did not refuse before native access", err)
		}
	}
	for _, initialized := range []bool{false, true} {
		var zeroHandle *WorkerDeployment
		var zeroLegacy *DeploymentClient
		if initialized {
			zeroHandle, zeroLegacy = &WorkerDeployment{}, &DeploymentClient{}
		}
		for _, call := range deploymentCapabilityCalls(client, zeroHandle, zeroLegacy) {
			if strings.HasPrefix(call.operation, "worker.") || call.operation == "deployment.list" || call.operation == "deployment.delete" {
				continue
			}
			if err := call.invoke(fixture.ctx); !errors.Is(err, ErrInput) || len(peer.requests) != 0 {
				t.Fatal(call.method, "zero handle failed to refuse safely", err)
			}
		}
	}
}
