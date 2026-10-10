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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"github.com/frost-leo/fathomry/internal/conformance"
	"go.temporal.io/sdk/converter"
)

func TestRuntimeValuesDoNotSerializeOrExposePrivateInputs(t *testing.T) {
	for _, value := range []any{Owner{}, Handle{}, Client{}, Binding{}, Prepared{}, NativeOptions{}, Dependencies{},
		WorkflowRun{}, WorkflowUpdate{}, WithStartWorkflowOperation{}, QueryValue{}, ActivityRun{}, ActivityDescription{}, NexusRun{}, NexusDescription{},
		NexusCancellation{}, Schedule{}, Worker{}, WorkerSpec{}, WorkerDeployment{}, DeploymentClient{}} {
		conformance.Runtime(t, value, reflect.New(reflect.TypeOf(value)).Interface())
	}
	settings := Settings{Name: "sensitive-canary", Endpoint: "sensitive-canary:7233", Namespace: "sensitive-canary", APIKey: "sensitive-canary"}
	conformance.Private(t, settings, "sensitive-canary")
	for _, value := range []any{settings, &settings, NativeOptions{DataConverter: converter.GetDefaultDataConverter()}} {
		if strings.Contains(fmt.Sprintf("%+v", value), "sensitive-canary") {
			t.Fatal("unsafe default formatting")
		}
	}
}

func TestPublicDiagnosticsRedactEvidenceButPreserveDeliberateNativeInspection(t *testing.T) {
	canary := "private-evidence-canary"
	cause := errors.New(canary)
	result := Result{Source: Attribution{Name: canary, Namespace: canary, SourceID: canary},
		Execution: Execution{WorkflowID: canary, RequestID: canary}, semantic: cause}
	values := []any{result, result.Source, result.Execution, RPCResult{Method: canary},
		WorkerResult{TaskQueue: canary, Namespace: canary}, TaskResult{WorkflowID: canary, ActivityID: canary}}
	for _, value := range values {
		if strings.Contains(fmt.Sprintf("%s %v %+v %#v", value, value, value, value), canary) {
			t.Fatal("ordinary evidence formatting exposed private identity")
		}
		var output bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&output, nil))
		logger.Log(context.Background(), slog.LevelInfo, "evidence", "value", value)
		if strings.Contains(output.String(), canary) {
			t.Fatal("slog evidence formatting exposed private identity")
		}
		conformance.Runtime(t, value, reflect.New(reflect.TypeOf(value)).Interface())
	}
	if native, present := result.NativeError(); !present || native != cause {
		t.Fatal("explicit native inspection lost exact error")
	}
	settings := Settings{Name: canary, Endpoint: canary + ":7233", Namespace: canary, APIKey: canary}
	encoded, err := json.Marshal(settings)
	if err != nil || !bytes.Contains(encoded, []byte(canary)) {
		t.Fatal("data-only configuration no longer supports explicit serialization", err)
	}
	var decoded Settings
	if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.APIKey != canary {
		t.Fatal("explicit sensitive configuration round trip changed", err)
	}
}
