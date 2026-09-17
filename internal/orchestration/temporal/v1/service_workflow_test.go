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

package temporal_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdk "go.temporal.io/sdk/client"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func TestAuthorizedWorkflowInteractionAndReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fixture := newExecutionServiceFixture(t, ctx, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 2, Bytes: 4 * fixture.envelope,
		Options: worker.Options{MaxConcurrentWorkflowTaskExecutionSize: 2, MaxConcurrentWorkflowTaskPollers: 2, WorkerStopTimeout: time.Second},
		Workflows: []temporal.WorkflowRegistration{
			{Definition: interactionWorkflow, Options: workflow.RegisterOptions{Name: "interaction-flow"}},
			{Definition: interactionChild, Options: workflow.RegisterOptions{Name: "interaction-child"}},
			{Definition: signalStartWorkflow, Options: workflow.RegisterOptions{Name: "signal-start"}},
		},
	}, fixture.workers, fixture.tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	var firstRunID string
	t.Cleanup(func() { fixture.cleanupWorkflow(t, fixture.prefix, firstRunID) })
	t.Cleanup(func() { fixture.cleanupWorkflow(t, fixture.prefix, "") })
	t.Cleanup(func() { fixture.cleanupWorkflow(t, fixture.prefix+"-child", "") })
	operation, err := fixture.executions.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: fixture.prefix, TaskQueue: fixture.prefix,
		WorkflowExecutionTimeout: 45 * time.Second, WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING},
		"interaction-flow", interactionInput{Value: 1})
	if err != nil {
		t.Fatal(err)
	}
	update, err := fixture.executions.UpdateWithStartWorkflow(ctx, fault.Correlation{Call: "combined-start"}, operation,
		sdk.UpdateWorkflowOptions{UpdateID: "add-once", UpdateName: "add", Args: []any{2}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		executionCauses(t, err, 0)
		t.Fatal(err)
	}
	var value int
	if err := update.Get(ctx, fault.Correlation{Call: "combined-result"}, &value); err != nil || value != 3 {
		t.Fatal("combined Update result differed from the expected initial value plus two")
	}
	run, err := operation.Get(ctx, fault.Correlation{Call: "started-run"})
	if err != nil {
		t.Fatal(err)
	}
	firstRunID = run.GetRunID()
	if firstRunID == "" || run.GetFirstExecutionRunID() != firstRunID {
		t.Fatal("initial execution-chain identity missing")
	}
	duplicate, err := fixture.executions.UpdateWorkflow(ctx, fault.Correlation{Call: "duplicate"}, sdk.UpdateWorkflowOptions{
		WorkflowID: fixture.prefix, RunID: firstRunID, UpdateID: "add-once", UpdateName: "add", Args: []any{100}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		t.Fatal(err)
	}
	if err := duplicate.Get(ctx, fault.Correlation{Call: "duplicate-result"}, &value); err != nil || value != 3 {
		t.Fatal("duplicate Update ID did not retain the original result")
	}
	rejected, err := fixture.executions.UpdateWorkflow(ctx, fault.Correlation{Call: "invalid-update"}, sdk.UpdateWorkflowOptions{
		WorkflowID: fixture.prefix, RunID: firstRunID, UpdateID: "rejected", UpdateName: "add", Args: []any{-1}, WaitForStage: sdk.WorkflowUpdateStageCompleted})
	if err != nil {
		t.Fatal(err)
	}
	var application *sdktemporal.ApplicationError
	if err := rejected.Get(ctx, fault.Correlation{Call: "rejected-result"}, &value); !errors.As(err, &application) {
		t.Fatal("native validator rejection did not remain an application error")
	}
	pending, err := fixture.executions.UpdateWorkflow(ctx, fault.Correlation{Call: "deferred-update"}, sdk.UpdateWorkflowOptions{
		WorkflowID: fixture.prefix, RunID: firstRunID, UpdateID: "deferred", UpdateName: "deferred", Args: []any{4}, WaitForStage: sdk.WorkflowUpdateStageAccepted})
	if err != nil {
		t.Fatal(err)
	}
	observation, stopWaiting := context.WithCancel(ctx)
	defer stopWaiting()
	waited := make(chan error, 1)
	go func() { waited <- pending.Get(observation, fault.Correlation{Call: "canceled-wait"}, nil) }()
	select {
	case err := <-waited:
		executionCauses(t, err, 0)
		t.Fatal("pending result returned before the observer canceled")
	case <-time.After(50 * time.Millisecond):
	}
	stopWaiting()
	err = <-waited
	if !errors.Is(err, context.Canceled) {
		executionCauses(t, err, 0)
		t.Fatalf("pending Update result wait did not end with its caller (nil error: %t)", err == nil)
	}
	if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "query-before-release"}, fixture.prefix, firstRunID, "value", &value); err != nil || value != 3 {
		t.Fatal("duplicate/rejected Update mutated workflow state")
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "release-update"}, fixture.prefix, firstRunID, "release-update", true); err != nil {
		t.Fatal(err)
	}
	if err := pending.Get(ctx, fault.Correlation{Call: "continued-update"}, &value); err != nil || value != 7 {
		t.Fatal("canceling the result waiter canceled or duplicated remote Update work")
	}
	queryRejection, err := fixture.executions.QueryWorkflowWithOptions(ctx, fault.Correlation{Call: "query-options"},
		&sdk.QueryWorkflowWithOptionsRequest{WorkflowID: fixture.prefix, RunID: firstRunID, QueryType: "value", QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN}, &value)
	if err != nil || queryRejection != nil || value != 7 {
		t.Fatal("open-workflow query options changed native semantics")
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "finish"}, fixture.prefix, firstRunID, "finish", true); err != nil {
		t.Fatal(err)
	}
	err = run.GetWithOptions(ctx, fault.Correlation{Call: "strict-result"}, nil, sdk.WorkflowRunGetOptions{DisableFollowingRuns: true})
	var continued *workflow.ContinueAsNewError
	if !errors.As(err, &continued) {
		t.Fatal("strict chain observation did not return native Continue-As-New")
	}
	if err := run.Get(ctx, fault.Correlation{Call: "chain-result"}, &value); err != nil || value != 21 {
		executionCauses(t, err, 0)
		t.Fatal("continued workflow/child/timer/markers disagreed with independent result 21")
	}
	finalRunID := run.GetRunID()
	if finalRunID == "" || finalRunID == firstRunID || run.GetFirstExecutionRunID() != firstRunID {
		t.Fatal("chain following lost first/current run separation")
	}
	queryRejection, err = fixture.executions.QueryWorkflowWithOptions(ctx, fault.Correlation{Call: "closed-query"},
		&sdk.QueryWorkflowWithOptionsRequest{WorkflowID: fixture.prefix, RunID: finalRunID, QueryType: "value", QueryRejectCondition: enumspb.QUERY_REJECT_CONDITION_NOT_OPEN}, &value)
	if err != nil || queryRejection.GetStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
		t.Fatal("native query rejection was lost or presented as a result")
	}
	initialHistory := fixture.history(t, ctx, fixture.prefix, firstRunID)
	finalHistory := fixture.history(t, ctx, fixture.prefix, finalRunID)
	child, err := fixture.executions.DescribeWorkflowExecution(ctx, fault.Correlation{Call: "child-metadata"}, fixture.prefix+"-child", "")
	if err != nil {
		t.Fatal(err)
	}
	childHistory := fixture.history(t, ctx, fixture.prefix+"-child", child.GetWorkflowExecutionInfo().GetExecution().GetRunId())
	for _, replay := range []struct {
		history           *historypb.History
		workflowID, runID string
	}{{initialHistory, fixture.prefix, firstRunID}, {finalHistory, fixture.prefix, finalRunID},
		{childHistory, fixture.prefix + "-child", child.GetWorkflowExecutionInfo().GetExecution().GetRunId()}} {
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(interactionWorkflow, workflow.RegisterOptions{Name: "interaction-flow"})
		replayer.RegisterWorkflowWithOptions(interactionChild, workflow.RegisterOptions{Name: "interaction-child"})
		if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, replay.history,
			worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: replay.workflowID, RunID: replay.runID}}); err != nil {
			t.Fatal("native interactive/continued/child history replay failed", err)
		}
	}
	rejecting := worker.NewWorkflowReplayer()
	rejecting.RegisterWorkflowWithOptions(incompatibleInteraction, workflow.RegisterOptions{Name: "interaction-flow"})
	rejecting.RegisterWorkflowWithOptions(interactionChild, workflow.RegisterOptions{Name: "interaction-child"})
	if err := rejecting.ReplayWorkflowHistoryWithOptions(executionLogger{}, finalHistory,
		worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: fixture.prefix, RunID: finalRunID}}); err == nil || !strings.Contains(err.Error(), "[TMPRL1100]") {
		t.Fatal("replay control did not reject the incompatible command as nondeterminism")
	}
	signalID := fixture.prefix + "-signal"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, signalID, "") })
	signaled, err := fixture.executions.SignalWithStartWorkflow(ctx, fault.Correlation{Call: "signal-start"}, signalID, "value", "delivered",
		sdk.StartWorkflowOptions{TaskQueue: fixture.prefix, WorkflowExecutionTimeout: 15 * time.Second}, "signal-start")
	if err != nil {
		t.Fatal(err)
	}
	var signalValue string
	if err := signaled.Get(ctx, fault.Correlation{Call: "signal-result"}, &signalValue); err != nil || signalValue != "delivered" {
		t.Fatal("combined Signal did not reach the new Workflow")
	}
	if err := managed.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for fixture.evidence.Usage().Outstanding > 0 {
		record := receiveExecution(t, fixture.evidence)
		switch record.Context.Correlation.Call {
		case "combined-start":
			seen["combined"] = record.Outcome.Value.StartAccepted && record.Outcome.Value.UpdateStage == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
		case "canceled-wait":
			seen["canceled"] = errors.Is(record.Err(), context.Canceled) && !record.Outcome.Value.ResultObtained
		case "rejected-result":
			seen["rejected"] = errors.As(record.Err(), &application)
		case "chain-result":
			seen["chain"] = record.Outcome.Value.RunID == finalRunID && record.Outcome.Value.ResultObtained
		case "closed-query":
			seen["query"] = record.Err() == nil && !record.Outcome.Value.ResultObtained
		}
	}
	for _, fact := range []string{"combined", "canceled", "rejected", "chain", "query"} {
		if !seen[fact] {
			t.Fatalf("independent evidence missing: %s", fact)
		}
	}
	t.Log("Native combined starts, Update deduplication/validation/wait cancellation, Query rejection, child/timer/markers and Continue-As-New passed; three histories replayed and incompatible commands were rejected")
}

type primitiveResult struct {
	Count, Version int
	Mutable        []int
	Random         []byte
	Number         uint64
}

func primitiveWorkflow(ctx workflow.Context, attribute string) (primitiveResult, error) {
	return primitiveBody(ctx, attribute, false)
}

func primitiveReplay(ctx workflow.Context, attribute string) (primitiveResult, error) {
	return primitiveBody(ctx, attribute, true)
}

func primitiveBody(ctx workflow.Context, attribute string, changedSideEffect bool) (primitiveResult, error) {
	var result primitiveResult
	ready := false
	if err := workflow.SetQueryHandler(ctx, "ready", func() (bool, error) { return ready, nil }); err != nil {
		return result, err
	}
	if err := workflow.SetQueryHandler(ctx, "readonly", func() (bool, error) { return workflow.IsReadOnly(ctx), nil }); err != nil {
		return result, err
	}
	if err := workflow.SetQueryHandler(ctx, "invalid-mutation", func() (bool, error) { workflow.NewTimer(ctx, time.Second); return false, nil }); err != nil {
		return result, err
	}
	mutex := workflow.NewMutex(ctx)
	semaphore := workflow.NewSemaphore(ctx, 1)
	canceled, cancel := workflow.WithCancel(ctx)
	cancel()
	if err := mutex.Lock(ctx); err != nil {
		return result, err
	}
	if mutex.TryLock(ctx) {
		return result, errors.New("native mutex admitted a second owner")
	}
	var canceledError *sdktemporal.CanceledError
	if err := mutex.Lock(canceled); !errors.As(err, &canceledError) {
		return result, errors.New("mutex canceled waiter did not fail")
	}
	mutex.Unlock()
	if err := semaphore.Acquire(ctx, 1); err != nil {
		return result, err
	}
	if semaphore.TryAcquire(ctx, 1) {
		return result, errors.New("native semaphore exceeded capacity")
	}
	if err := semaphore.Acquire(canceled, 1); !errors.As(err, &canceledError) {
		return result, errors.New("semaphore canceled waiter did not fail")
	}
	semaphore.Release(1)
	if err := workflow.NewTimer(canceled, time.Hour).Get(ctx, nil); !errors.As(err, &canceledError) {
		return result, errors.New("canceled native timer did not fail")
	}
	disconnected, finishDisconnected := workflow.NewDisconnectedContext(canceled)
	defer finishDisconnected()
	if err := workflow.NewTimer(disconnected, time.Millisecond).Get(ctx, nil); err != nil {
		return result, err
	}
	group := workflow.NewWaitGroup(ctx)
	for range 2 {
		group.Go(ctx, func(child workflow.Context) {
			if err := mutex.Lock(child); err != nil {
				return
			}
			result.Count++
			mutex.Unlock()
		})
	}
	group.Wait(ctx)
	future, settable := workflow.NewFuture(ctx)
	settable.Set(7, nil)
	selector := workflow.NewSelector(ctx)
	selector.AddFuture(future, func(done workflow.Future) { var value int; _ = done.Get(ctx, &value); result.Count += value })
	selector.Select(ctx)
	for _, next := range []int{1, 1, 2} {
		value := next
		var actual int
		err := workflow.MutableSideEffect(ctx, "gh61/mutable", func(workflow.Context) any {
			if changedSideEffect {
				return 99
			}
			return value
		}, func(left, right any) bool { return left == right }).Get(&actual)
		if err != nil {
			return result, err
		}
		result.Mutable = append(result.Mutable, actual)
	}
	result.Version = int(workflow.GetVersion(ctx, "gh61/primitive-version", workflow.DefaultVersion, 2))
	random := workflow.GetRandomStream(ctx, "gh61/random")
	result.Random = make([]byte, 8)
	if _, err := random.Read(result.Random[:3]); err != nil {
		return result, err
	}
	result.Number = random.Uint64()
	if _, err := workflow.GetRandomStream(ctx, "gh61/random").Read(result.Random[3:]); err != nil {
		return result, err
	}
	key := sdktemporal.NewSearchAttributeKeyKeyword(attribute)
	if err := workflow.UpsertTypedSearchAttributes(ctx, key.ValueSet("updated")); err != nil {
		return result, err
	}
	value, exists := workflow.GetTypedSearchAttributes(ctx).GetKeyword(key)
	if !exists || value != "updated" {
		return result, errors.New("native typed attribute update lost")
	}
	if err := workflow.UpsertMemo(ctx, map[string]any{"phase": "updated"}); err != nil {
		return result, err
	}
	workflow.SetCurrentDetails(ctx, "waiting at primitive checkpoint")
	if workflow.GetCurrentDetails(ctx) != "waiting at primitive checkpoint" {
		return result, errors.New("native current details lost")
	}
	ready = true
	var finish bool
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	if err := workflow.UpsertTypedSearchAttributes(ctx, key.ValueUnset()); err != nil {
		return result, err
	}
	if _, exists := workflow.GetTypedSearchAttributes(ctx).GetKeyword(key); exists {
		return result, errors.New("native attribute unset lost")
	}
	return result, nil
}

func keywordForFixture(t *testing.T, ctx context.Context, fixture *executionServiceFixture) string {
	t.Helper()
	service := fixture.raw.OperatorService(fault.Correlation{Call: "attribute-schema"})
	schema, err := service.ListSearchAttributes(ctx, &operatorservice.ListSearchAttributesRequest{Namespace: fixture.namespace})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for name, kind := range schema.CustomAttributes {
		if kind == enumspb.INDEXED_VALUE_TYPE_KEYWORD {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	if len(names) > 0 {
		return names[0]
	}
	name := "Gh61Keyword" + strings.ReplaceAll(fixture.prefix, "-", "")[4:16]
	if _, err := service.AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{Namespace: fixture.namespace, SearchAttributes: map[string]enumspb.IndexedValueType{name: enumspb.INDEXED_VALUE_TYPE_KEYWORD}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		schema, err := service.ListSearchAttributes(cleanup, &operatorservice.ListSearchAttributesRequest{Namespace: fixture.namespace})
		if err != nil || schema.GetCustomAttributes()[name] != enumspb.INDEXED_VALUE_TYPE_KEYWORD {
			t.Error("owned schema cannot be verified before removal")
			return
		}
		if _, err := service.RemoveSearchAttributes(cleanup, &operatorservice.RemoveSearchAttributesRequest{Namespace: fixture.namespace, SearchAttributes: []string{name}}); err != nil {
			t.Error(err)
			return
		}
		schema, err = service.ListSearchAttributes(cleanup, &operatorservice.ListSearchAttributesRequest{Namespace: fixture.namespace})
		if err != nil {
			t.Error(err)
			return
		}
		if _, exists := schema.CustomAttributes[name]; exists {
			t.Error("owned schema removal was not observed")
		}
	})
	return name
}

func TestAuthorizedNativePrimitivesMetadataAndReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	operator := "/temporal.api.operatorservice.v1.OperatorService/"
	fixture := newExecutionServiceFixture(t, ctx, operator+"ListSearchAttributes", operator+"AddSearchAttributes", operator+"RemoveSearchAttributes",
		workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	attribute := keywordForFixture(t, ctx, fixture)
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "primitive-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 2, Bytes: 2 * fixture.envelope,
		Options:   worker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2},
		Workflows: []temporal.WorkflowRegistration{{Definition: primitiveWorkflow, Options: workflow.RegisterOptions{Name: "primitive-workflow"}}},
	}, fixture.workers, fixture.tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	id := fixture.prefix + "-workflow"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
	run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "start"}, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix,
		WorkflowExecutionTimeout: 45 * time.Second, StaticSummary: "native primitive fixture", StaticDetails: "deterministic metadata",
		Memo: map[string]any{"phase": "initial"}, TypedSearchAttributes: sdktemporal.NewSearchAttributes(sdktemporal.NewSearchAttributeKeyKeyword(attribute).ValueSet("initial"))}, "primitive-workflow", attribute)
	if err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		var ready bool
		if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "ready"}, id, run.GetRunID(), "ready", &ready); err != nil {
			t.Fatal(err)
		}
		releaseServiceEvidence(t, fixture.evidence)
		if ready {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("primitive checkpoint unavailable")
		}
	}
	var readonly bool
	if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "readonly"}, id, run.GetRunID(), "readonly", &readonly); err != nil || !readonly {
		t.Fatal("query was not read-only", err)
	}
	if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "bad-query"}, id, run.GetRunID(), "invalid-mutation", &readonly); err == nil {
		t.Fatal("query emitted a native timer command")
	}
	description, err := fixture.executions.DescribeWorkflow(ctx, fault.Correlation{Call: "metadata"}, id, run.GetRunID())
	if err != nil {
		t.Fatal(err)
	}
	summary, err := description.GetStaticSummary(ctx, fault.Correlation{Call: "summary"})
	if err != nil || summary != "native primitive fixture" {
		t.Fatal("static metadata mismatch", err)
	}
	var phase string
	if err := description.GetMemoValue(ctx, fault.Correlation{Call: "memo"}, "phase", &phase); err != nil || phase != "updated" {
		t.Fatal("memo update not observed", err)
	}
	for {
		response, err := fixture.executions.ListWorkflow(ctx, fault.Correlation{Call: "visibility"}, &workflowservice.ListWorkflowExecutionsRequest{
			Query: "WorkflowId = '" + id + "' AND " + attribute + " = 'updated'", PageSize: 1})
		if err != nil {
			t.Fatal(err)
		}
		releaseServiceEvidence(t, fixture.evidence)
		if len(response.Executions) == 1 && response.Executions[0].Execution.WorkflowId == id {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("updated search attribute never became visible")
		}
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "finish"}, id, run.GetRunID(), "finish", true); err != nil {
		t.Fatal(err)
	}
	var result primitiveResult
	if err := run.Get(ctx, fault.Correlation{Call: "result"}, &result); err != nil {
		executionCauses(t, err, 0)
		t.Fatal(err)
	}
	seed := sha256.Sum256([]byte("temporal.sdk.random.v1\x00" + run.GetRunID() + "\x00gh61/random"))
	random := rand.NewChaCha8(seed)
	expected := make([]byte, 16)
	if _, err := random.Read(expected); err != nil {
		t.Fatal(err)
	}
	expectedBytes := append(append([]byte{}, expected[:3]...), expected[11:]...)
	if result.Count != 9 || result.Version != 2 || !reflect.DeepEqual(result.Mutable, []int{1, 1, 2}) || !reflect.DeepEqual(result.Random, expectedBytes) || result.Number != binary.LittleEndian.Uint64(expected[3:11]) {
		t.Fatal("primitive result differs from independent oracle")
	}
	history := fixture.history(t, ctx, id, run.GetRunID())
	mutableMarkers := 0
	for _, event := range history.Events {
		if event.GetMarkerRecordedEventAttributes().GetMarkerName() == "MutableSideEffect" {
			mutableMarkers++
		}
	}
	if mutableMarkers != 2 {
		t.Fatalf("mutable markers=%d want=2", mutableMarkers)
	}
	for _, definition := range []any{primitiveWorkflow, primitiveReplay} {
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(definition, workflow.RegisterOptions{Name: "primitive-workflow"})
		if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}); err != nil {
			t.Fatal("native primitive history replay failed", err)
		}
	}
	t.Log("Native synchronization/cancellation, named random byte oracle, mutable/version markers, memo/static metadata, typed search attributes and readonly rejection passed; changed SideEffect callback replay consumed stored values")
}

type sessionActor struct{ label string }

func (actor *sessionActor) Label(context.Context) (string, error) { return actor.label, nil }

type sessionInput struct {
	Queue    string
	Token    []byte
	Previous string
	Recover  bool
}

type sessionResult struct {
	Labels    []string
	Recreated bool
}

func sessionWorkflow(ctx workflow.Context, input sessionInput) (sessionResult, error) {
	var result sessionResult
	phase := "starting"
	if err := workflow.SetQueryHandler(ctx, "phase", func() (string, error) { return phase, nil }); err != nil {
		return result, err
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: input.Queue, StartToCloseTimeout: 10 * time.Second, ScheduleToStartTimeout: 5 * time.Second,
		RetryPolicy: &sdktemporal.RetryPolicy{MaximumAttempts: 1}})
	options := &workflow.SessionOptions{CreationTimeout: 5 * time.Second, ExecutionTimeout: time.Minute, HeartbeatTimeout: 2 * time.Second}
	var session workflow.Context
	var err error
	if len(input.Token) == 0 {
		session, err = workflow.CreateSession(ctx, options)
	} else {
		session, err = workflow.RecreateSession(ctx, input.Token, options)
	}
	if err != nil {
		return result, err
	}
	defer workflow.CompleteSession(session)
	info := workflow.GetSessionInfo(session)
	for range 2 {
		var label string
		if err := workflow.ExecuteActivity(session, "session.Label").Get(ctx, &label); err != nil {
			return result, err
		}
		result.Labels = append(result.Labels, label)
	}
	if input.Recover {
		phase = "ready"
		if err := workflow.Await(ctx, func() bool { return workflow.GetSessionInfo(session).SessionState == workflow.SessionStateFailed }); err != nil {
			return result, err
		}
		failedOptions := workflow.GetActivityOptions(session)
		failedOptions.ActivityID = "must-not-schedule"
		failed := workflow.WithActivityOptions(session, failedOptions)
		if err := workflow.ExecuteActivity(failed, "session.Label").Get(ctx, nil); !errors.Is(err, workflow.ErrSessionFailed) {
			return result, errors.New("failed session admitted an Activity")
		}
		phase = "failed"
		var recover bool
		workflow.GetSignalChannel(ctx, "recover").Receive(ctx, &recover)
		recovered, err := workflow.CreateSession(ctx, options)
		if err != nil {
			return result, err
		}
		defer workflow.CompleteSession(recovered)
		var label string
		if err := workflow.ExecuteActivity(recovered, "session.Label").Get(ctx, &label); err != nil {
			return result, err
		}
		result.Labels = append(result.Labels, label)
		return result, nil
	}
	if len(input.Token) == 0 {
		input.Token = info.GetRecreateToken()
		input.Previous = info.SessionID
		workflow.CompleteSession(session)
		return result, workflow.NewContinueAsNewError(ctx, "session-workflow", input)
	}
	result.Recreated = input.Previous != info.SessionID
	return result, nil
}

func startSessionTestWorker(t *testing.T, ctx context.Context, fixture *executionServiceFixture, queue, label string) *temporal.Worker {
	t.Helper()
	results, err := invocation.NewInbox[temporal.WorkerResult](1, temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := invocation.NewInbox[temporal.TaskResult](32, 32*temporal.ExecutionEvidenceBytes)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseServiceEvidence(t, results); releaseServiceEvidence(t, tasks) })
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	spec := temporal.WorkerSpec{TaskQueue: queue, MaxHandlers: 6, Bytes: 3 * fixture.envelope}
	if label == "" {
		spec.Options = worker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2}
		spec.Workflows = []temporal.WorkflowRegistration{{Definition: sessionWorkflow, Options: workflow.RegisterOptions{Name: "session-workflow"}}}
	} else {
		spec.Options = worker.Options{DisableWorkflowWorker: true, EnableSessionWorker: true, MaxConcurrentSessionExecutionSize: 1,
			MaxConcurrentActivityExecutionSize: 6, MaxConcurrentActivityTaskPollers: 2, WorkerStopTimeout: time.Second}
		spec.Activities = []temporal.ActivityRegistration{{Definition: &sessionActor{label: label}, Options: activity.RegisterOptions{Name: "session."}}}
	}
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "session-worker-" + label}, spec, results, tasks)
	if managed != nil {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
			defer stop()
			if err := managed.Stop(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return managed
}

func TestAuthorizedSessionAffinityRecreateAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fixture := newExecutionServiceFixture(t, ctx, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	workflowQueue, activityQueue := fixture.prefix+"-workflow", fixture.prefix+"-session"
	startSessionTestWorker(t, ctx, fixture, workflowQueue, "")
	actorA := startSessionTestWorker(t, ctx, fixture, activityQueue, "A")
	start := func(suffix string, recover bool) *temporal.WorkflowRun {
		t.Helper()
		id := fixture.prefix + "-" + suffix
		t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
		run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: suffix}, sdk.StartWorkflowOptions{ID: id, TaskQueue: workflowQueue, WorkflowExecutionTimeout: 90 * time.Second},
			"session-workflow", sessionInput{Queue: activityQueue, Recover: recover})
		if err != nil {
			t.Fatal(err)
		}
		return run
	}
	recreated := start("recreate", false)
	firstRun := recreated.GetRunID()
	t.Cleanup(func() { fixture.cleanupWorkflow(t, recreated.GetID(), firstRun) })
	var result sessionResult
	if err := recreated.Get(ctx, fault.Correlation{Call: "recreate-result"}, &result); err != nil {
		executionCauses(t, err, 0)
		t.Fatal(err)
	}
	if !result.Recreated || !reflect.DeepEqual(result.Labels, []string{"A", "A"}) {
		t.Fatal("session recreate did not retain the correct Worker instance")
	}
	recovering := start("recover", true)
	observePhase := func(want string) {
		t.Helper()
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			var phase string
			if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "phase"}, recovering.GetID(), recovering.GetRunID(), "phase", &phase); err != nil {
				t.Fatal(err)
			}
			releaseServiceEvidence(t, fixture.evidence)
			if phase == want {
				return
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				t.Fatal("session phase did not converge")
			}
		}
	}
	observePhase("ready")
	if err := actorA.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	observePhase("failed")
	startSessionTestWorker(t, ctx, fixture, activityQueue, "B")
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "recover-signal"}, recovering.GetID(), recovering.GetRunID(), "recover", true); err != nil {
		t.Fatal(err)
	}
	if err := recovering.Get(ctx, fault.Correlation{Call: "recovery-result"}, &result); err != nil {
		executionCauses(t, err, 0)
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Labels, []string{"A", "A", "B"}) {
		t.Fatal("session recovery did not select the new Worker")
	}
	for _, execution := range []workflow.Execution{{ID: recreated.GetID(), RunID: firstRun}, {ID: recreated.GetID(), RunID: recreated.GetRunID()}, {ID: recovering.GetID(), RunID: recovering.GetRunID()}} {
		history := fixture.history(t, ctx, execution.ID, execution.RunID)
		for _, event := range history.Events {
			if event.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED && event.GetActivityTaskScheduledEventAttributes().GetActivityId() == "must-not-schedule" {
				t.Fatal("failed Session emitted an Activity command")
			}
		}
		replayer := worker.NewWorkflowReplayer()
		replayer.RegisterWorkflowWithOptions(sessionWorkflow, workflow.RegisterOptions{Name: "session-workflow"})
		if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: execution}); err != nil {
			t.Fatal("Session history replay failed", err)
		}
	}
	t.Log("Native Session affinity, Continue-As-New recreation, Worker-loss failure/no-command rejection and recovery on another instance passed with three histories replayed")
}
