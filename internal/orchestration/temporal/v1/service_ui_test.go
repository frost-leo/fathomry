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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func uiCodecWorkflow(ctx workflow.Context, value string) (string, error) {
	if err := workflow.SetQueryHandler(ctx, "greeting", func() (string, error) { return value, nil }); err != nil {
		return "", err
	}
	var finish bool
	workflow.GetSignalChannel(ctx, "finish").Receive(ctx, &finish)
	return value + ":finished", nil
}

// This separately opted-in fixture remains owned while a local browser inspects
// its exact synthetic execution. The done marker only releases it; browser
// assertions are recorded separately and are not inferred by this Go test.
func TestAuthorizedUIHistoryAndCodecFixture(t *testing.T) {
	artifacts := os.Getenv("FATHOMRY_TEMPORAL_UI_ARTIFACTS")
	if artifacts == "" {
		t.Skip("explicit local UI fixture artifacts directory required")
	}
	info, err := os.Stat(artifacts)
	if err != nil || !info.IsDir() {
		t.Fatal("UI artifact directory must already exist")
	}
	config := authorizedServiceConfiguration(t)
	target, err := url.Parse(config.Temporal.UIURL)
	if err != nil || target.Host == "" || target.Scheme != "http" && target.Scheme != "https" {
		t.Fatal("configured UI origin is invalid")
	}
	origin := target.Scheme + "://" + target.Host
	codec := versionCodec{version: "2", legacy: true}
	handler := converter.NewPayloadCodecHTTPHandler(codec)
	codecServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Origin") != "" && request.Header.Get("Origin") != origin {
			http.Error(writer, "origin refused", http.StatusForbidden)
			return
		}
		writer.Header().Set("Access-Control-Allow-Origin", origin)
		writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Namespace, X-Workflow-Id, X-Run-Id, X-Temporal-Namespace, X-Temporal-Workflow-Id")
		writer.Header().Set("Access-Control-Allow-Private-Network", "true")
		if request.Method == http.MethodOptions {
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		handler.ServeHTTP(writer, request)
	}))
	t.Cleanup(codecServer.Close)
	runtime := temporal.RuntimeOptions{DataConverter: converter.NewCodecDataConverter(converter.GetDefaultDataConverter(), codec)}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	fixture := newRuntimeExecutionServiceFixture(t, ctx, runtime, workflowPrefix+"GetWorkflowExecutionHistory", workflowPrefix+"DeleteWorkflowExecution")
	lifetime, stopLifetime := context.WithCancel(context.Background())
	t.Cleanup(stopLifetime)
	managed, err := fixture.executions.StartWorker(ctx, lifetime, fault.Correlation{Call: "ui-worker"}, temporal.WorkerSpec{
		TaskQueue: fixture.prefix, MaxHandlers: 2, Bytes: 2 * fixture.envelope,
		Options:   worker.Options{LocalActivityWorkerOnly: true, MaxConcurrentWorkflowTaskExecutionSize: 4, MaxConcurrentWorkflowTaskPollers: 2},
		Workflows: []temporal.WorkflowRegistration{{Definition: uiCodecWorkflow, Options: workflow.RegisterOptions{Name: "ui-codec-workflow"}}},
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
	id := fixture.prefix + "-ui"
	t.Cleanup(func() { fixture.cleanupWorkflow(t, id, "") })
	run, err := fixture.executions.ExecuteWorkflow(ctx, fault.Correlation{Call: "ui-start"}, sdk.StartWorkflowOptions{ID: id, TaskQueue: fixture.prefix,
		WorkflowExecutionTimeout: 10 * time.Minute, StaticSummary: "Fathomry SDK acceptance", StaticDetails: "Native codec and history fixture",
		Memo: map[string]any{"scenario": "gh61-ui-codec"}}, "ui-codec-workflow", "gh61-visible-payload")
	if err != nil {
		t.Fatal(err)
	}
	var greeting string
	if err := fixture.executions.QueryWorkflow(ctx, fault.Correlation{Call: "ui-ready"}, id, run.GetRunID(), "greeting", &greeting); err != nil || greeting != "gh61-visible-payload" {
		t.Fatal("UI fixture query was not ready", err)
	}
	target.Path = "/namespaces/" + url.PathEscape(fixture.namespace) + "/workflows/" + url.PathEscape(id) + "/" + url.PathEscape(run.GetRunID()) + "/history"
	record := map[string]any{"ui_url": target.String(), "namespace": fixture.namespace, "workflow_id": id, "run_id": run.GetRunID(),
		"codec_url": codecServer.URL, "expected_payload": "gh61-visible-payload", "expected_memo": "gh61-ui-codec", "query": "greeting",
		"sdk": "1.49.0", "server": "1.32.0", "ui": "2.54.1", "codec_profile": "fixture-v2-legacy-reader"}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	ready := filepath.Join(artifacts, "target.json")
	if err := os.WriteFile(ready, data, 0600); err != nil {
		t.Fatal(err)
	}
	t.Log("Exact synthetic UI fixture and loopback codec are ready; waiting for browser completion marker")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, err := os.Stat(filepath.Join(artifacts, "browser-done"))
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("browser fixture deadline reached")
		}
	}
	if err := fixture.executions.SignalWorkflow(ctx, fault.Correlation{Call: "ui-finish"}, id, run.GetRunID(), "finish", true); err != nil {
		t.Fatal(err)
	}
	var result string
	if err := run.Get(ctx, fault.Correlation{Call: "ui-result"}, &result); err != nil || result != "gh61-visible-payload:finished" {
		t.Fatal("UI fixture did not complete", err)
	}
	history := fixture.history(t, ctx, id, run.GetRunID())
	replayer, err := worker.NewWorkflowReplayerWithOptions(worker.WorkflowReplayerOptions{DataConverter: runtime.DataConverter})
	if err != nil {
		t.Fatal(err)
	}
	replayer.RegisterWorkflowWithOptions(uiCodecWorkflow, workflow.RegisterOptions{Name: "ui-codec-workflow"})
	if err := replayer.ReplayWorkflowHistoryWithOptions(executionLogger{}, history, worker.ReplayWorkflowHistoryOptions{OriginalExecution: workflow.Execution{ID: id, RunID: run.GetRunID()}}); err != nil {
		t.Fatal("UI-profile history replay failed", err)
	}
	t.Log("UI fixture completed and exact history replayed with the selected native codec; execution cleanup is owned")
}
