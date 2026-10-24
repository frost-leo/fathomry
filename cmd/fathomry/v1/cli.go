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

package cli

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	createcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/errorcatalog"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/messages"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project"
	buildcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project/build"
	schemacmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	applycmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema/apply"
	plancmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema/plan"
	statuscmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema/status"
	workflowcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	comparecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/compare"
	listcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/list"
	registercmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/register"
	showcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/show"
	vetcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow/vet"
	"github.com/frost-leo/fathomry/failure/v1"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	wf "github.com/frost-leo/fathomry/framework/workflow/declaration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/spf13/cobra"
)

// Options supplies explicit invocation dependencies. All streams are required.
// Empty Language selects English; zero CleanupTimeout selects five seconds.
// Errors and Messages must either both be nil (the minimal built-in catalog), or
// both be prepared with Components plus the caller's chosen component metadata.
type Options struct {
	Input          io.Reader
	Output         io.Writer
	ErrorOutput    io.Writer
	Language       string
	CleanupTimeout time.Duration
	Errors         *failure.Catalog
	Messages       *i18n.Catalog
}

// Project supplies the root executable's offline data and optional lazy control
// configuration. Control is called only by explicit online commands, never by
// construction, help, vet, build or build inspection. Name is a portable project
// name. Workflows is the combined declaration/metadata registry; legacy
// Declarations remains supported, but setting both is invalid. Empty input means
// a configuration-only project, not a completed workflow check. Assembly and init
// must not access services or run business operations.
type Project struct {
	Name         string
	Workflows    []Workflow
	Declarations []wf.Declaration
	Control      DatabaseResolver
}

// Workflow combines one logical workflow's authored versions and optional catalog
// metadata. Use Project.Workflows or legacy Declarations, not both. Each registry
// admits at most declaration.MaxInputs entries; versions remain inside their entry.
type Workflow = workflowcmd.Workflow

// ExecutionError retains an online command's original error, bounded JSON mutation
// report and resource cleanup continuation. Use errors.As on a returned error.
// ReportCopy is detached; Close with a fresh context can join unfinished cleanup.
// CleanupComplete is local release, not proof of rollback or remote lock release.
type ExecutionError = command.ExecutionError

// Components returns the fixed command set's metadata without preparing a catalog.
// Callers with a custom catalog add their base/component data separately.
func Components() []i18n.Component {
	result := []i18n.Component{command.Component(), errorcatalog.Component(), messages.Component(), wf.Component(), catalog.Component(), vetcmd.Component()}
	result = append(result, project.Components()...)
	result = append(result, schemacmd.Components()...)
	result = append(result, statuscmd.Component(), plancmd.Component(), applycmd.Component())
	result = append(result, databasecmd.Components()...)
	result = append(result, createcmd.Component())
	result = append(result, registercmd.Component())
	result = append(result, listcmd.Component(), showcmd.Component())
	result = append(result, comparecmd.Component())
	return append(result, workflowcmd.Components()...)
}

// Run executes the fixed CLI command set. Unknown commands fail rather than invoke
// application code. Build runs trusted project code in a temporary offline process;
// it is not a sandbox or a Temporal execution-admission guarantee.
func Run(ctx context.Context, args []string, options Options) error {
	return invoke(ctx, args, options, commands(Project{}))
}

// Handle handles known CLI commands, global CLI flags, and the inspection protocol
// before project startup. No arguments or an unrecognized first word returns false
// without reading options or doing work, so application arguments remain project-owned.
// When handled is true, the caller must not fall through even if err is non-nil.
func Handle(ctx context.Context, args []string, project Project, options Options) (handled bool, err error) {
	if len(args) == 0 {
		return false, nil
	}
	if strings.HasPrefix(args[0], "--fathomry-project") {
		// Machine inspection is independent of inherited human display preferences.
		options.Language = "en"
		return true, invoke(ctx, nil, options, func(invocation *command.Invocation) *cobra.Command {
			root := &cobra.Command{Use: "fathomry", Short: "fathomry.command_line.root", Args: cobra.NoArgs}
			invocation.Bind(root, func(ctx context.Context, _ []string) error {
				if len(args) != 1 || args[0] != buildcmd.InspectionArgument {
					return command.Fail(command.ErrUsage)
				}
				return buildcmd.Export(ctx, buildcmd.Definition{Name: project.Name, Workflows: project.Workflows, Declarations: project.Declarations}, options.Output)
			})
			return root
		})
	}
	first, _, _ := strings.Cut(args[0], "=")
	switch first {
	case "help", "--help", "-h", "--lang", "--output":
		return true, invoke(ctx, args, options, commands(project))
	}
	for _, selected := range commands(project)(&command.Invocation{}).Commands() {
		if selected.Name() == args[0] {
			return true, invoke(ctx, args, options, commands(project))
		}
	}
	return false, nil
}

// ExitCode applies the same process policy used by the official executable.
func ExitCode(err error) int { return command.ExitCode(err) }

func invoke(ctx context.Context, args []string, options Options, builder func(*command.Invocation) *cobra.Command) error {
	catalogs := command.Catalogs{Errors: options.Errors, Messages: options.Messages}
	if catalogs.Errors == nil && catalogs.Messages == nil {
		components := append(i18n.CoreComponents(), Components()...)
		var definitions []failure.Definition
		for _, component := range components {
			definitions = append(definitions, component.Definitions...)
		}
		var err error
		catalogs.Errors, err = failure.Prepare(definitions...)
		if err == nil {
			catalogs.Messages, err = i18n.Prepare(components...)
		}
		if err != nil {
			return command.Unavailable(options.ErrorOutput, err)
		}
	}
	return command.Run(ctx, args, command.Options{
		Input: options.Input, Output: options.Output, ErrorOutput: options.ErrorOutput,
		Language: options.Language, CleanupTimeout: options.CleanupTimeout,
	}, catalogs, builder)
}

func commands(definition Project) func(*command.Invocation) *cobra.Command {
	return func(invocation *command.Invocation) *cobra.Command {
		read := func(ctx context.Context) (result schemacmd.Status, err error) {
			err = withDatabase(ctx, invocation, definition.Control, func(runner *schemacmd.Runner) error { result, err = runner.Status(ctx); return err })
			return
		}
		inspect := func(ctx context.Context) (result schemacmd.Plan, err error) {
			err = withDatabase(ctx, invocation, definition.Control, func(runner *schemacmd.Runner) error { result, err = runner.Plan(ctx); return err })
			return
		}
		install := func(ctx context.Context) (result schemacmd.Change, err error) {
			err = withDatabase(ctx, invocation, definition.Control, func(runner *schemacmd.Runner) error { result, err = runner.Apply(ctx); return err })
			return
		}
		create := func(ctx context.Context, path string) (result createcmd.DatabaseChange, err error) {
			err = withProvisioner(ctx, invocation, definition.Control, path, func(runner *createcmd.Creator) error { result, err = runner.Create(ctx); return err })
			return
		}
		root := invocation.Group("fathomry", "fathomry.command_line.root")
		var inspectProject func(context.Context, workflowcmd.Selection) (workflowcmd.ProjectReport, error)
		if definition.Name != "" || len(definition.Workflows) > 0 || len(definition.Declarations) > 0 {
			inspectProject = func(ctx context.Context, selection workflowcmd.Selection) (workflowcmd.ProjectReport, error) {
				return workflowcmd.InspectProject(ctx, workflowcmd.ProjectInput{Workflows: definition.Workflows, Declarations: definition.Declarations}, selection)
			}
		}
		workflows := workflowcmd.New(invocation)
		workflows.AddCommand(vetcmd.New(invocation, inspectProject))
		workflows.AddCommand(registercmd.New(invocation, func(ctx context.Context, selection workflowcmd.Selection) (workflowcmd.PublicationReport, error) {
			return registerProject(ctx, invocation, definition, selection)
		}))
		workflows.AddCommand(listcmd.New(invocation, func(ctx context.Context, request catalog.ListRequest) (catalog.Page, error) {
			return listCatalog(ctx, invocation, definition.Control, request)
		}))
		workflows.AddCommand(showcmd.New(invocation, func(ctx context.Context, request catalog.ReadRequest) (catalog.View, error) {
			return readCatalog(ctx, invocation, definition.Control, request)
		}))
		workflows.AddCommand(comparecmd.New(invocation, func(ctx context.Context, selection workflowcmd.Selection) (workflowcmd.ComparisonReport, error) {
			return compareProject(ctx, invocation, definition, selection)
		}))
		schemas := schemacmd.New(invocation)
		schemas.AddCommand(statuscmd.New(invocation, read), plancmd.New(invocation, inspect), applycmd.New(invocation, install))
		databases := databasecmd.New(invocation)
		databases.AddCommand(createcmd.New(invocation, create))
		root.AddCommand(errorcatalog.New(invocation), messages.New(invocation), workflows, schemas, databases)
		root.AddCommand(project.Commands(invocation)...)
		return root
	}
}
