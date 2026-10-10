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
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	sdktemporal "go.temporal.io/sdk/temporal"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type descriptionStorage struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	reads   atomic.Int32
}

func (*descriptionStorage) Name() string { return "description-fixture" }
func (*descriptionStorage) Type() string { return "test" }
func (*descriptionStorage) Store(converter.StorageDriverStoreContext, []*commonpb.Payload) ([]converter.StorageDriverClaim, error) {
	return nil, errors.New("description test never stores")
}
func (store *descriptionStorage) Retrieve(_ converter.StorageDriverRetrieveContext, claims []converter.StorageDriverClaim) ([]*commonpb.Payload, error) {
	store.reads.Add(1)
	store.once.Do(func() { close(store.entered) })
	<-store.release
	result := make([]*commonpb.Payload, len(claims))
	for index := range result {
		result[index], _ = converter.GetDefaultDataConverter().ToPayload("decoded")
	}
	return result, nil
}

type descriptionServer struct {
	*testServer
	reference *commonpb.Payload
}

func (server *descriptionServer) DescribeActivityExecution(context.Context, *workflowservice.DescribeActivityExecutionRequest) (*workflowservice.DescribeActivityExecutionResponse, error) {
	return &workflowservice.DescribeActivityExecutionResponse{RunId: "run", Info: &activitypb.ActivityExecutionInfo{
		ActivityId: "activity", RunId: "run", ActivityType: &commonpb.ActivityType{Name: "fixture"}, SearchAttributes: &commonpb.SearchAttributes{},
	}, Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{cloneMessage(server.reference)}}}, nil
}
func (server *descriptionServer) DescribeNexusOperationExecution(context.Context, *workflowservice.DescribeNexusOperationExecutionRequest) (*workflowservice.DescribeNexusOperationExecutionResponse, error) {
	failure := &failurepb.Failure{Message: "fixture failure", FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
		Type: "fixture", Details: &commonpb.Payloads{Payloads: []*commonpb.Payload{cloneMessage(server.reference)}},
	}}}
	return &workflowservice.DescribeNexusOperationExecutionResponse{Info: &nexuspb.NexusOperationExecutionInfo{OperationId: "operation", RunId: "run",
		CancellationInfo: &nexuspb.NexusOperationExecutionCancellationInfo{Reason: "fixture", LastAttemptFailure: failure}}}, nil
}

func descriptionFixture(t *testing.T) (*Owner, *descriptionStorage) {
	t.Helper()
	reference, err := converter.GetDefaultDataConverter().ToPayload(&sdkpb.ExternalStorageReference{DriverName: "description-fixture", ClaimData: map[string]string{"id": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	workflowservice.RegisterWorkflowServiceServer(server, &descriptionServer{testServer: &testServer{}, reference: reference})
	joined := make(chan struct{})
	go func() { defer close(joined); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-joined })
	store := &descriptionStorage{entered: make(chan struct{}), release: make(chan struct{})}
	native := NativeOptions{ExternalStorage: converter.ExternalStorage{Drivers: []converter.StorageDriver{store}}}
	prepared, err := Prepare(Settings{Name: "descriptions", Endpoint: listener.Addr().String(), Namespace: "test", Plaintext: true,
		MaxActive: 2, MaxRequestBytes: 4096, MaxResponseBytes: 4096, InnerEvidenceCapacity: 16}, native)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := prepared.Policy()
	if err != nil {
		t.Fatal(err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	runtime, err := adapters.New(lifetime, policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := adapters.NewInbox[WorkerResult](policy.Workers)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := adapters.NewInbox[TaskResult](policy.Tasks)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := prepared.Open(lifetime, Dependencies{Runtime: runtime, Evidence: evidence, Workers: workers, Tasks: tasks})
	if err != nil {
		cancel()
		if owner != nil {
			_ = owner.Close(context.Background())
		}
		_ = runtime.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		select {
		case <-store.release:
		default:
			close(store.release)
		}
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if err := owner.Close(ctx); err != nil {
			t.Error(err)
		}
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return owner, store
}

func TestActivityMetadataIsImmutableAcrossLazyRetrieval(t *testing.T) {
	owner, store := descriptionFixture(t)
	client := owner.Client()
	run, err := client.GetActivityHandle(sdk.GetActivityHandleOptions{ActivityID: "activity", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	description, err := run.Describe(context.Background(), sdk.DescribeActivityOptions{IncludeInput: true})
	if err != nil {
		t.Fatal(err)
	}
	baseline := description.Metadata()
	if !description.HasInput() {
		t.Fatal("native presence was lost")
	}
	done := make(chan error, 1)
	var decoded string
	go func() { done <- description.GetInput(context.Background(), &decoded) }()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("lazy external retrieval never entered")
	}
	started, stop, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readDone)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			value := description.Metadata()
			value.RawResponse.Input.Payloads[0].Data = []byte("caller mutation")
			_ = description.HasInput()
			runtime.Gosched()
		}
	}()
	<-started
	close(store.release)
	decodeErr := <-done
	close(stop)
	<-readDone
	if decodeErr != nil {
		t.Fatal("lazy decode", decodeErr)
	}
	if decoded != "decoded" || store.reads.Load() != 1 || !proto.Equal(baseline.RawResponse, description.Metadata().RawResponse) {
		t.Fatal("description metadata aliased a mutable native decoder")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := description.GetInput(context.Background(), &decoded); err == nil || store.reads.Load() != 1 {
		t.Fatal("expired description decoder remained usable", err)
	}
}

func TestDescriptionMetadataFieldsMatchSelectedNative(t *testing.T) {
	for _, profile := range []struct {
		name           string
		native, public reflect.Type
		rename         map[string]string
	}{
		{"activity", reflect.TypeFor[sdk.ActivityExecutionDescription](), reflect.TypeFor[ActivityMetadata](), nil},
		{"nexus", reflect.TypeFor[sdk.NexusOperationExecutionDescription](), reflect.TypeFor[NexusMetadata](), map[string]string{"CancellationInfo": "Cancellation"}},
		{"nexus-cancellation", reflect.TypeFor[sdk.NexusOperationCancellationInfo](), reflect.TypeFor[NexusCancellationMetadata](), nil},
	} {
		t.Run(profile.name, func(t *testing.T) {
			var check func(reflect.Type)
			check = func(native reflect.Type) {
				for index := range native.NumField() {
					field := native.Field(index)
					if !field.IsExported() {
						continue
					}
					if field.Anonymous && field.Type.Kind() == reflect.Struct {
						check(field.Type)
						continue
					}
					name := field.Name
					if renamed, ok := profile.rename[name]; ok {
						name = renamed
					}
					public, ok := profile.public.FieldByName(name)
					if !ok || !public.IsExported() {
						t.Errorf("selected native data field %s has no public metadata counterpart", field.Name)
					} else if name == field.Name && public.Type != field.Type {
						t.Errorf("selected native field %s type changed: native %s, public %s", name, field.Type, public.Type)
					}
				}
			}
			check(profile.native)
		})
	}
}

func TestNexusCancellationMetadataIsImmutableAcrossLazyFailure(t *testing.T) {
	owner, store := descriptionFixture(t)
	client := owner.Client()
	run, err := client.GetNexusOperationHandle(sdk.GetNexusOperationHandleOptions{OperationID: "operation", RunID: "run"})
	if err != nil {
		t.Fatal(err)
	}
	description, err := run.Describe(context.Background(), sdk.DescribeNexusOperationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cancellation := description.Cancellation()
	if cancellation == nil {
		t.Fatal("native nested cancellation was lost")
	}
	baseline := description.Metadata()
	done := make(chan error, 1)
	go func() { done <- cancellation.GetLastAttemptFailure(context.Background()) }()
	select {
	case <-store.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("nested cancellation retrieval never entered")
	}
	started, stop, readDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readDone)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
			}
			value := description.Metadata()
			value.Cancellation.RawInfo.LastAttemptFailure.Message = "caller mutation"
			runtime.Gosched()
		}
	}()
	<-started
	close(store.release)
	observed := <-done
	close(stop)
	<-readDone
	semantic, ok := NativeError(observed)
	var application *sdktemporal.ApplicationError
	if !ok || !errors.As(semantic, &application) {
		t.Fatal("nested native failure semantics lost", observed)
	}
	var details string
	if err := application.Details(&details); err != nil || details != "decoded" {
		t.Fatal("nested failure details lost", err)
	}
	if store.reads.Load() != 1 || !proto.Equal(baseline.RawInfo, description.Metadata().RawInfo) ||
		!proto.Equal(baseline.Cancellation.RawInfo, description.Metadata().Cancellation.RawInfo) {
		t.Fatal("nested cancellation metadata aliased lazy native failure")
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cancellation.GetLastAttemptFailure(context.Background()); err == nil || store.reads.Load() != 1 {
		t.Fatal("expired nested decoder remained usable", err)
	}
}
