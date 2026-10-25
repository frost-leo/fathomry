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

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/framework/workflow/declaration/v1/testdata/health"
)

func TestUnifiedCommandEntry(t *testing.T) {
	for _, args := range [][]string{{"build", "--help"}, {"new", "--help"}, {"workflow", "vet", "--help"}, {"error", "list", "--output=json"}, {"--lang", "zh-CN", "build", "--help"}, {"help", "workflow", "vet"}} {
		var official, local, officialError, localError bytes.Buffer
		first := cli.Run(context.Background(), args, cli.Options{Input: strings.NewReader(""), Output: &official, ErrorOutput: &officialError})
		handled, second := cli.Handle(context.Background(), args, cli.Project{Name: "health"}, cli.Options{Input: strings.NewReader(""), Output: &local, ErrorOutput: &localError})
		if !handled || first != nil || second != nil || !bytes.Equal(official.Bytes(), local.Bytes()) || !bytes.Equal(officialError.Bytes(), localError.Bytes()) {
			t.Fatal(args, handled, first, second)
		}
	}
	for _, args := range [][]string{nil, {"--env", "production"}, {"application-command"}} {
		if handled, err := cli.Handle(nil, args, cli.Project{}, cli.Options{}); handled || err != nil {
			t.Fatal("application startup was intercepted", args, err)
		}
	}
}

func TestLazyProjectControl(t *testing.T) {
	cause := errors.New("private-control-canary")
	calls := 0
	project := cli.Project{Name: "health", Control: func(context.Context) (cli.DatabaseConfiguration, error) {
		calls++
		return cli.DatabaseConfiguration{}, cause
	}}
	for _, args := range [][]string{
		{"schema", "--help"}, {"schema", "apply", "--help"}, {"--lang", "zh-CN", "schema", "plan", "--help"},
		{"--output=json", "help", "schema", "status"}, {"workflow", "vet"}, {"build", "--unknown"},
		{"--fathomry-project=v1"}, {"error", "list", "--output=json"}, {"messages", "--help"},
	} {
		var output, diagnostic bytes.Buffer
		_, _ = cli.Handle(context.Background(), args, project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		if calls != 0 {
			t.Fatal("offline command resolved control", args)
		}
	}
	for _, args := range [][]string{
		{"schema", "status"}, {"schema", "plan"}, {"schema", "apply"},
		{"--lang", "zh-CN", "--output=json", "schema", "status"}, {"--output=json", "schema", "apply"},
	} {
		before := calls
		var output, diagnostic bytes.Buffer
		handled, err := cli.Handle(context.Background(), args, project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		core, known := failure.Inspect(err)
		var execution *cli.ExecutionError
		if !handled || calls != before+1 || !errors.Is(err, cause) || !known || core.Diagnostic().Definition.Identifier != "fathomry.database_control.configuration_unavailable" ||
			!errors.As(err, &execution) || !execution.CleanupComplete() || len(execution.ReportCopy()) != 0 || output.Len() != 0 {
			t.Fatal("lazy handoff or online error ownership changed", args, handled, err)
		}
		if diagnostic.Len() == 0 || strings.Contains(diagnostic.String(), "private-control-canary") {
			t.Fatal("unsafe online diagnostic")
		}
	}
	for _, leaf := range []string{"status", "plan", "apply"} {
		var output, diagnostic bytes.Buffer
		err := cli.Run(context.Background(), []string{"schema", leaf, "--output=json"}, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		core, known := failure.Inspect(err)
		if !known || core.Diagnostic().Definition.Identifier != "fathomry.database_control.configuration_unavailable" || output.Len() != 0 || diagnostic.Len() == 0 {
			t.Fatal("standalone operation discovered ambient control", leaf, err)
		}
	}
}

func TestProjectInspectionAndFailures(t *testing.T) {
	project := cli.Project{Name: "health", Declarations: []wf.Declaration{health.Declarations()}}
	var output, diagnostic bytes.Buffer
	options := cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}
	options.Language = "invalid_private_locale"
	handled, err := cli.Handle(context.Background(), []string{"--fathomry-project=v1"}, project, options)
	var document struct {
		Schema  string            `json:"schema"`
		Name    string            `json:"name"`
		Bundles []json.RawMessage `json:"bundles"`
	}
	if !handled || err != nil || diagnostic.Len() != 0 || json.Unmarshal(output.Bytes(), &document) != nil || document.Name != "health" || len(document.Bundles) != 1 {
		t.Fatal(handled, err, output.String(), diagnostic.String())
	}
	options.Language = "en"
	for _, args := range [][]string{{"--fathomry-project=v3"}, {"--fathomry-project=v1", "private-canary"}, {"build", "--unknown=private-canary"}, {"--output=json", "build", "--unknown=private-canary"}} {
		output.Reset()
		diagnostic.Reset()
		handled, err := cli.Handle(context.Background(), args, project, options)
		if !handled || err == nil || cli.ExitCode(err) != 2 || output.Len() != 0 || diagnostic.Len() == 0 || strings.Contains(diagnostic.String(), "private-canary") {
			t.Fatal(args, handled, err, diagnostic.String())
		}
	}
}

func TestProjectRegisterValidatesBeforeResolvingDatabase(t *testing.T) {
	first, second := authoredWorkflow("healthy"), authoredWorkflow("broken")
	second.Declaration.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "9999"}
	calls := 0
	cause := errors.New("private-register-resolver")
	project := cli.Project{Name: "application", Workflows: []cli.Workflow{first, second}, Control: func(context.Context) (cli.DatabaseConfiguration, error) {
		calls++
		return cli.DatabaseConfiguration{}, cause
	}}
	for _, args := range [][]string{
		{"workflow", "register", "--help"},
		{"--lang=zh-CN", "help", "workflow", "register"},
		{"--output=json", "workflow", "register"},
		{"--output=json", "workflow", "register", "--mode=patient"},
	} {
		var output, diagnostic bytes.Buffer
		handled, _ := cli.Handle(context.Background(), args, project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		if !handled || calls != 0 {
			t.Fatal("help or rejected preflight acquired database configuration")
		}
		if strings.Contains(output.String()+diagnostic.String(), "private-register") {
			t.Fatal("native detail leaked")
		}
	}
	var output, diagnostic bytes.Buffer
	handled, err := cli.Handle(context.Background(), []string{"--output=json", "workflow", "register", "--workflow=healthy"}, project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
	if !handled || calls != 1 || !errors.Is(err, cause) {
		t.Fatal("valid selection did not invoke exactly one lazy resolver", err)
	}
	var report struct {
		Data struct {
			Checked   bool
			Attempted int
			Effect    string
			Selected  int `json:"selected_workflows"`
		}
	}
	if err := json.Unmarshal(output.Bytes(), &report); err != nil || !report.Data.Checked || report.Data.Attempted != 0 || report.Data.Selected != 1 || report.Data.Effect != "not_started" {
		t.Fatal("resolver failure changed mutation facts", err)
	}
}
func authoredWorkflow(code string) cli.Workflow {
	declared := health.Declarations()
	declared.Workflows[0].Code = code
	metadata := &catalog.Metadata{Resources: []catalog.Resource{{Target: catalog.Target{Kind: catalog.Definition}, Field: catalog.Name, Text: catalog.Text{English: "Health"}}}}
	for _, mode := range declared.Workflows[0].Modes {
		metadata.Resources = append(metadata.Resources, catalog.Resource{Target: catalog.Target{Kind: catalog.Mode, Version: "1", Mode: mode.Code}, Field: catalog.Name, Text: catalog.Text{English: mode.Code}})
	}
	return cli.Workflow{Declaration: declared, Metadata: metadata}
}

func TestProjectVetScopeAndOfflineBehavior(t *testing.T) {
	first, second := authoredWorkflow("healthy"), authoredWorkflow("broken")
	second.Declaration.Workflows[0].Modes[0].Values = map[string]wf.Decimal{"attempt_timeout": "9999"}
	calls := 0
	project := cli.Project{Name: "application", Workflows: []cli.Workflow{first, second}, Control: func(context.Context) (cli.DatabaseConfiguration, error) {
		calls++
		return cli.DatabaseConfiguration{}, errors.New("private-resolver-canary")
	}}
	for _, test := range []struct {
		args         []string
		passed, full bool
		selected     int
	}{
		{[]string{"workflow", "vet"}, false, true, 2},
		{[]string{"workflow", "vet", "--workflow=healthy"}, true, false, 1},
		{[]string{"--lang=zh-CN", "workflow", "vet", "--namespace=example", "--workflow=healthy", "--version=1", "--mode=patient", "--revision=1"}, true, false, 1},
	} {
		var output, diagnostic bytes.Buffer
		handled, err := cli.Handle(context.Background(), append(test.args, "--output=json"), project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		// Preserve the wire names independently of the internal command types.
		var decoded struct {
			Command string
			Data    struct {
				Format    string
				Passed    bool
				Full      bool `json:"full_project"`
				Total     int  `json:"total_workflows"`
				Selected  int  `json:"selected_workflows"`
				Workflows []struct{ Code, Metadata string }
			}
		}
		if json.Unmarshal(output.Bytes(), &decoded) != nil || !handled || decoded.Command != "workflow.vet" || decoded.Data.Format != "fathomry.project-workflow-check/v2" || decoded.Data.Passed != test.passed || decoded.Data.Full != test.full || decoded.Data.Total != 2 || decoded.Data.Selected != test.selected || calls != 0 {
			t.Fatal("wrong project vet scope", err, output.String())
		}
		if (err == nil) != test.passed {
			t.Fatal("wrong check outcome", err)
		}
	}
	for _, args := range [][]string{
		{"workflow", "vet", "--workflow=absent"},
		{"workflow", "vet", "--version=1"},
		{"workflow", "vet", "--workflow="},
		{"workflow", "vet", "private-path-canary", "--workflow=healthy"},
	} {
		var output, diagnostic bytes.Buffer
		_, err := cli.Handle(context.Background(), args, project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		if err == nil || cli.ExitCode(err) != 2 || output.Len() != 0 || strings.Contains(diagnostic.String(), "canary") || calls != 0 {
			t.Fatal("invalid selector performed work", args, err)
		}
	}
	for _, locale := range []string{"en", "zh-CN"} {
		var output, diagnostic bytes.Buffer
		_, err := cli.Handle(context.Background(), []string{"workflow", "vet", "--workflow=healthy", "--lang=" + locale}, project, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic})
		if err != nil || !strings.Contains(output.String(), "healthy") || strings.Contains(output.String(), "fathomry.workflow_vet.") || diagnostic.Len() != 0 {
			t.Fatal("project result localization failed", err)
		}
	}
}

func TestProjectVetDoesNotWeakenFullBuildInspection(t *testing.T) {
	first, second := authoredWorkflow("healthy"), authoredWorkflow("broken")
	second.Metadata.Resources[0].Text.English = ""
	project := cli.Project{Name: "application", Workflows: []cli.Workflow{first, second}}
	var output, diagnostic bytes.Buffer
	options := cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostic}
	if _, err := cli.Handle(context.Background(), []string{"workflow", "vet", "--workflow=healthy"}, project, options); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	diagnostic.Reset()
	if _, err := cli.Handle(context.Background(), []string{"--fathomry-project=v1"}, project, options); err == nil || output.Len() != 0 {
		t.Fatal("selected success bypassed full metadata admission", err)
	}
	second.Metadata.Resources[0].Text.English = "Health"
	output.Reset()
	diagnostic.Reset()
	if _, err := cli.Handle(context.Background(), []string{"--fathomry-project=v1"}, project, options); err != nil || output.Len() == 0 {
		t.Fatal("valid full registration rejected", err)
	}
	output.Reset()
	diagnostic.Reset()
	if _, err := cli.Handle(context.Background(), []string{"workflow", "vet", "--output=json"}, cli.Project{Name: "empty"}, options); err == nil || !bytes.Contains(output.Bytes(), []byte(`"empty":true`)) || !bytes.Contains(output.Bytes(), []byte(`"passed":false`)) {
		t.Fatal("empty project claimed verification", err)
	}
	output.Reset()
	diagnostic.Reset()
	if err := cli.Run(context.Background(), []string{"workflow", "vet"}, options); err == nil || output.Len() != 0 {
		t.Fatal("standalone CLI discovered a project")
	}
}
