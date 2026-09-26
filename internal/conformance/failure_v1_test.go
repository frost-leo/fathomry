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

package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/cli"
	"github.com/frost-leo/fathomry/failure/v1"
	viper "github.com/frost-leo/fathomry/internal/configsource/viper/v1"
	"github.com/frost-leo/fathomry/internal/fault"
	sdk "github.com/spf13/viper"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const primaryCode failure.Condition = "example.source.absent"
const cleanupCode failure.Condition = "another.owner.cleanup"

type parseReceipt struct{ native error }

// The mapping owner retains private evidence after returning; it deliberately
// does not expose the private implementation graph through the public failure.
func fixtureLoad(raw string) ([]*viper.Document, error, parseReceipt) {
	documents, native := viper.Load(context.Background(), []viper.LoadInput{{
		Options: viper.OptionsV1{Encoding: "json"}, Reader: strings.NewReader(raw),
	}})
	if native == nil {
		return documents, nil, parseReceipt{}
	}
	public, rejected := failure.New("example.document.rejected")
	if rejected != nil {
		panic(rejected)
	}
	return nil, public, parseReceipt{native: native}
}

func TestPublicFailureActualParserPromotionAndPrivateRetention(t *testing.T) {
	const canary = "synthetic_parser_secret"
	documents, public, receipt := fixtureLoad(`{"secret":"` + canary + `",`)
	if documents != nil || public == nil || receipt.native == nil {
		t.Fatal("missing rejection/retention")
	}
	var technical *fault.Error
	var native sdk.ConfigParseError
	var syntax *json.SyntaxError
	if !errors.As(receipt.native, &technical) || technical.Diagnostic().Context.Operation != "decode" || technical.Diagnostic().Context.Provider != viper.ProviderID ||
		!errors.Is(receipt.native, viper.ErrDecode) || !errors.As(receipt.native, &native) || !errors.As(receipt.native, &syntax) || syntax.Offset == 0 {
		t.Fatal("not the selected native JSON rejection phase")
	}
	if errors.Is(public, viper.ErrDecode) || errors.As(public, &technical) || errors.As(public, &native) || errors.As(public, &syntax) {
		t.Fatal("private implementation promoted")
	}
	if !errors.Is(public, failure.Condition("example.document.rejected")) || strings.Contains(fmt.Sprintf("%#v", public), canary) {
		t.Fatal("public promotion")
	}
	documents, public, receipt = fixtureLoad(`{"secret":"` + canary + `","enabled":true}`)
	if public != nil || receipt.native != nil || len(documents) != 1 {
		t.Fatal("valid control rejected")
	}
	value, err := documents[0].ValueCopy("enabled")
	if err != nil || value != true {
		t.Fatal("valid native decode was not exercised")
	}
	// Caller-owned SDK-typed evidence can be deliberately exposed by role.
	callerOwned := native
	exposed := newPublicFailure(t, "example.reader.failed", callerOwned)
	if !errors.As(exposed, &native) || errors.Unwrap(native) != errors.Unwrap(callerOwned) {
		t.Fatal("SDK type blacklisted")
	}
	t.Log("malformed JSON: private fault operation=decode; native Viper ConfigParseError and json.SyntaxError retained only by receipt; valid control decoded")
}

func TestPublicFailureIndependentFactsAndCLIJoin(t *testing.T) {
	absent := newPublicFailure(t, primaryCode)
	cleanup := newPublicFailure(t, cleanupCode, context.Canceled)
	type sourceOutcome struct {
		Primary, Cleanup error
		Absent           bool
	}
	outcome := sourceOutcome{Primary: absent, Cleanup: cleanup, Absent: true}
	for _, combined := range []error{errors.Join(outcome.Primary, outcome.Cleanup), errors.Join(outcome.Cleanup, outcome.Primary)} {
		if !errors.Is(combined, primaryCode) || !errors.Is(combined, cleanupCode) || !errors.Is(combined, context.Canceled) {
			t.Fatal("independent cause erased")
		}
		if _, ok := failure.Inspect(combined); ok {
			t.Fatal("join acquired primary")
		}
		if !outcome.Absent || outcome.Cleanup == nil {
			t.Fatal("absence erased cleanup")
		}
	}
	type effect struct {
		Accepted    int
		CommitKnown bool
		Err         error
	}
	partial := effect{Accepted: 3, CommitKnown: false, Err: newPublicFailure(t, "example.write.uncertain", context.DeadlineExceeded)}
	if !errors.Is(partial.Err, context.DeadlineExceeded) || partial.Accepted != 3 || partial.CommitKnown {
		t.Fatal("timeout changed effect facts")
	}
	type credentialObservation struct {
		ActiveGeneration, RequestGeneration uint64
		Err                                 error
	}
	for _, observed := range []error{nil, newPublicFailure(t, "example.credentials.refresh_failed"), newPublicFailure(t, "example.credentials.rejected")} {
		generation := credentialObservation{ActiveGeneration: 12, RequestGeneration: 11, Err: observed}
		if generation.ActiveGeneration != 12 || generation.RequestGeneration != 11 {
			t.Fatal("failure rebound generation")
		}
	}
	for _, reason := range []string{"complete-empty", "filtered", "superseded"} {
		normal := struct {
			Reason string
			Err    error
		}{Reason: reason}
		if normal.Err != nil || normal.Reason == "" {
			t.Fatal("normal outcome needs an error")
		}
	}
	var stdout, stderr strings.Builder
	status, err := cli.Run(context.Background(), []string{"missing"}, cli.Streams{Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr})
	if status != 2 || err == nil {
		t.Fatal("CLI rejecting control")
	}
	if _, ok := failure.Inspect(err); ok {
		t.Fatal("CLI aggregate acquired occurrence")
	}
}

func TestPublicFailureSelectedTemporalConverterBoundary(t *testing.T) {
	primary := newPublicFailure(t, primaryCode)
	cleanup := newPublicFailure(t, cleanupCode, context.Canceled)
	first := newPublicFailure(t, "example.operation.first", primary, cleanup)
	second := newPublicFailure(t, "example.operation.second", cleanup, primary)
	converter := temporal.GetDefaultFailureConverter()
	for _, current := range []*failure.Error{first, second} {
		if !errors.Is(current, primaryCode) || !errors.Is(current, cleanupCode) || !errors.Is(current, context.Canceled) {
			t.Fatal("in-process evidence missing")
		}
		wire := converter.ErrorToFailure(current)
		application := wire.GetApplicationFailureInfo()
		if application == nil || application.GetType() != "Error" || wire.GetMessage() != current.Error() || application.GetDetails() != nil || wire.GetCause() != nil || application.GetNonRetryable() {
			t.Fatalf("selected converter behavior changed: %v", wire)
		}
		restored := converter.FailureToError(wire)
		if _, ok := failure.Inspect(restored); ok {
			t.Fatal("converter unexpectedly implements v1")
		}
		if errors.Is(restored, current.Diagnostic().Condition) || errors.Is(restored, primaryCode) || errors.Is(restored, context.Canceled) {
			t.Fatal("assumed portable matching")
		}
		var native *temporal.ApplicationError
		if !errors.As(restored, &native) || native.Type() != "Error" || native.HasDetails() {
			t.Fatal("native conversion changed")
		}
		t.Logf("condition=%s native_type=%s message_retained=true details=false wire_cause=false non_retryable=false; restored v1 matching=false", current.Diagnostic().Condition, application.GetType())
	}
	nativeCancellation := temporal.NewCanceledError()
	withNativeCancellation := newPublicFailure(t, "example.operation.failed", primary, newPublicFailure(t, cleanupCode, nativeCancellation))
	var canceled *temporal.CanceledError
	if !errors.As(withNativeCancellation, &canceled) || canceled != nativeCancellation {
		t.Fatal("intentional native cancellation unreachable")
	}
	t.Log("cleanup context.Canceled and Temporal CanceledError remain visible to Go matching; native poller routing was source-inspected, not executed")
}

type quietTemporalLogger struct{}

func (quietTemporalLogger) Debug(string, ...any) {}
func (quietTemporalLogger) Info(string, ...any)  {}
func (quietTemporalLogger) Warn(string, ...any)  {}
func (quietTemporalLogger) Error(string, ...any) {}

type partialObservation struct {
	Successful   int
	NativeFailed int
	V1Failed     int
	NativeType   string
	NativeDetail string
	V1Type       string
}

func partialActivity(_ context.Context, mode string) (int, error) {
	switch mode {
	case "native":
		return 7, temporal.NewNonRetryableApplicationError("probe partial output", "probe.partial", nil, "probe.output.reference")
	case "v1":
		err, rejected := failure.New("example.write.partial")
		if rejected != nil {
			return 0, rejected
		}
		return 7, err
	default:
		return 7, nil
	}
}

func partialWorkflow(ctx workflow.Context) (partialObservation, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 1}})
	result := partialObservation{Successful: -1, NativeFailed: -1, V1Failed: -1}
	if err := workflow.ExecuteActivity(ctx, partialActivity, "success").Get(ctx, &result.Successful); err != nil {
		return result, err
	}
	nativeErr := workflow.ExecuteActivity(ctx, partialActivity, "native").Get(ctx, &result.NativeFailed)
	var application *temporal.ApplicationError
	if !errors.As(nativeErr, &application) {
		return result, errors.New("fixture missing native failure")
	}
	result.NativeType = application.Type()
	if err := application.Details(&result.NativeDetail); err != nil {
		return result, err
	}
	v1Err := workflow.ExecuteActivity(ctx, partialActivity, "v1").Get(ctx, &result.V1Failed)
	if !errors.As(v1Err, &application) {
		return result, errors.New("fixture missing v1 conversion")
	}
	result.V1Type = application.Type()
	return result, nil
}

func TestPublicFailureSelectedTemporalPartialResult(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(quietTemporalLogger{})
	environment := suite.NewTestWorkflowEnvironment()
	environment.SetTestTimeout(5 * time.Second)
	environment.RegisterActivity(partialActivity)
	environment.ExecuteWorkflow(partialWorkflow)
	if !environment.IsWorkflowCompleted() || environment.GetWorkflowError() != nil {
		t.Fatal("fixture workflow failed", environment.GetWorkflowError())
	}
	var result partialObservation
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatal(err)
	}
	if result.Successful != 7 || result.NativeFailed != -1 || result.V1Failed != -1 || result.NativeType != "probe.partial" || result.NativeDetail != "probe.output.reference" || result.V1Type != "Error" {
		t.Fatalf("partial-result control changed: %+v", result)
	}
	t.Logf("SDK-local only, activity returned 7 each time: %+v", result)
}

func newPublicFailure(t *testing.T, condition failure.Condition, causes ...error) *failure.Error {
	t.Helper()
	err, rejected := failure.New(condition, causes...)
	if rejected != nil {
		t.Fatal(rejected)
	}
	return err
}
