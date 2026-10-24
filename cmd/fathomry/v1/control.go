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
	"errors"
	"path/filepath"
	"strings"

	mysql "github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	postgres "github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	vocabulary "github.com/frost-leo/fathomry/adapters/database/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	databasecmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database"
	createcmd "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/database/create"
	schema "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/schema"
	workflows "github.com/frost-leo/fathomry/cmd/fathomry/internal/command/workflow"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	catalog "github.com/frost-leo/fathomry/framework/workflow/catalog/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type databaseSelection struct {
	target, admin DatabaseConfiguration
	create        bool
	workspace     string
	workBytes     int64
}

func (value databaseSelection) copy() databaseSelection {
	value.target, value.admin = value.target.copy(), value.admin.copy()
	return value
}

type controlClients struct {
	pg, adminPG       *postgres.Client
	mysql, adminMySQL *mysql.Client
}
type controlOperation interface {
	Close(context.Context) error
	Released() bool
}

func withDatabase(ctx context.Context, invocation *command.Invocation, resolver DatabaseResolver, run func(*schema.Runner) error) error {
	invocation.Managed()
	selected, err := resolveDatabase(ctx, resolver)
	if err != nil {
		return err
	}
	return withOwned(ctx, invocation, databaseSelection{target: selected, workspace: "schema.workspace", workBytes: schema.WorkBytes}, resource.Binding[databaseSelection, *schema.Runner]{
		Name: "control-schema", Clone: databaseSelection.copy,
		Build: func(lifetime context.Context, value databaseSelection) (*resource.Instance[*schema.Runner], error) {
			return buildControl(lifetime, value, func(clients controlClients) (*schema.Runner, error) {
				options := databasecmd.Options{Timeout: value.target.Timeout, CleanupTimeout: value.target.CleanupTimeout}
				if clients.pg != nil {
					return schema.PostgreSQL(clients.pg, value.target.Schema, options)
				}
				return schema.MySQL(clients.mysql, value.target.MySQL.Database, options)
			})
		},
	}, run)
}

func withProvisioner(ctx context.Context, invocation *command.Invocation, resolver DatabaseResolver, path string, run func(*createcmd.Creator) error) error {
	invocation.Managed()
	selected, err := resolveDatabase(ctx, resolver)
	if err != nil {
		return err
	}
	admin, err := loadAdministration(ctx, path)
	if err != nil {
		return controlFailure(err)
	}
	administrative, err := administrativeDatabase(selected, admin)
	if err != nil {
		return err
	}
	return withOwned(ctx, invocation, databaseSelection{target: selected, admin: administrative, create: true, workspace: "database.create.workspace", workBytes: createcmd.WorkBytes}, resource.Binding[databaseSelection, *createcmd.Creator]{
		Name: "control-create", Clone: databaseSelection.copy,
		Build: func(lifetime context.Context, value databaseSelection) (*resource.Instance[*createcmd.Creator], error) {
			return buildControl(lifetime, value, func(clients controlClients) (*createcmd.Creator, error) {
				options := creationOptions(value.target)
				if clients.pg != nil {
					return createcmd.PostgreSQLCreation(clients.adminPG, clients.pg, options)
				}
				return createcmd.MySQLCreation(clients.adminMySQL, clients.mysql, options)
			})
		},
	}, run)
}

func databasePolicy(value DatabaseConfiguration) (vocabulary.Policy, error) {
	if value.PostgreSQL != nil {
		return postgres.Recommend(*value.PostgreSQL)
	}
	return mysql.Recommend(*value.MySQL)
}

func registerProject(ctx context.Context, invocation *command.Invocation, project Project, selection workflows.Selection) (workflows.PublicationReport, error) {
	report, prepared, err := workflows.PrepareProject(ctx, workflows.ProjectInput{Workflows: project.Workflows, Declarations: project.Declarations}, selection)
	if err != nil {
		return report, err
	}
	var preparedBytes int64
	for _, request := range prepared {
		info, problem := request.Inspect()
		if problem != nil {
			return report, problem
		}
		preparedBytes += int64(info.DocumentBytes + info.MetadataBytes)
	}
	err = withCatalog(ctx, invocation, project.Control, preparedBytes, 0, func(value *catalog.Catalog) error {
		var problem error
		report, problem = workflows.PublishPrepared(ctx, report, prepared, value.Register)
		return problem
	})
	return report, err
}

func withCatalog(ctx context.Context, invocation *command.Invocation, resolver DatabaseResolver, preparedBytes int64, minimumRows int, run func(*catalog.Catalog) error) error {
	invocation.Managed()
	selected, err := resolveDatabase(ctx, resolver)
	if err != nil {
		return err
	}
	// This explicit catalog operation opts into the existing full-document profile.
	// Configured smaller limits are not silently widened and fail before SQL work.
	if selected.PostgreSQL != nil {
		value := selected.PostgreSQL
		if minimumRows > 0 {
			if value.MaxRows == 0 {
				value.MaxRows = max(1024, minimumRows)
			}
			if value.MaxRows < minimumRows {
				return databaseFailure(databasecmd.ErrLimit, "catalog_budget", catalog.ErrLimit)
			}
		}
		if value.MaxArgumentBytes == 0 {
			value.MaxArgumentBytes = 16 << 20
		}
		if value.MaxMessageBytes == 0 {
			value.MaxMessageBytes = 16 << 20
		}
		if value.MaxResultBytes == 0 {
			value.MaxResultBytes = 16 << 20
		}
		if value.MaxArgumentBytes < 16<<20 || value.MaxMessageBytes < 16<<20 || value.MaxResultBytes < 16<<20 {
			return databaseFailure(databasecmd.ErrLimit, "catalog_budget", catalog.ErrLimit)
		}
	} else {
		value := selected.MySQL
		if minimumRows > 0 {
			if value.MaxRows == 0 {
				value.MaxRows = max(1024, minimumRows)
			}
			if value.MaxRows < minimumRows {
				return databaseFailure(databasecmd.ErrLimit, "catalog_budget", catalog.ErrLimit)
			}
		}
		if value.MaxArgumentBytes == 0 {
			value.MaxArgumentBytes = 8 << 20
		}
		if value.MaxPacketBytes == 0 {
			value.MaxPacketBytes = 8 << 20
		}
		if value.MaxResultBytes == 0 {
			value.MaxResultBytes = 16 << 20
		}
		if value.MaxResponseBytes == 0 {
			value.MaxResponseBytes = 32 << 20
		}
		if value.MaxArgumentBytes < 8<<20 || value.MaxPacketBytes < 8<<20 || value.MaxResultBytes < 16<<20 || value.MaxResponseBytes < 32<<20 {
			return databaseFailure(databasecmd.ErrLimit, "catalog_budget", catalog.ErrLimit)
		}
	}
	return withOwned(ctx, invocation, databaseSelection{target: selected, workspace: "catalog.workspace", workBytes: int64(catalog.WorkBytes) + preparedBytes}, resource.Binding[databaseSelection, *catalog.Catalog]{
		Name: "workflow-catalog", Clone: databaseSelection.copy,
		Build: func(lifetime context.Context, value databaseSelection) (*resource.Instance[*catalog.Catalog], error) {
			return buildControl(lifetime, value, func(clients controlClients) (*catalog.Catalog, error) {
				options := catalog.Options{Timeout: value.target.Timeout, CleanupTimeout: value.target.CleanupTimeout}
				if clients.pg != nil {
					return catalog.PostgreSQL(clients.pg, value.target.Schema, options)
				}
				return catalog.MySQL(clients.mysql, value.target.MySQL.Database, options)
			})
		},
	}, run)
}

func compareProject(ctx context.Context, invocation *command.Invocation, project Project, selection workflows.Selection) (workflows.ComparisonReport, error) {
	report, prepared, err := workflows.PrepareComparison(ctx, workflows.ProjectInput{Workflows: project.Workflows, Declarations: project.Declarations}, selection)
	if err != nil {
		return report, err
	}
	var preparedBytes int64
	minimumRows := 0
	for _, request := range prepared {
		info, problem := request.Inspect()
		if problem != nil {
			return report, problem
		}
		preparedBytes += int64(info.DocumentBytes + info.MetadataBytes)
		minimumRows = max(minimumRows, 1+info.Versions+info.Revisions+info.Resources)
	}
	err = withCatalog(ctx, invocation, project.Control, preparedBytes, minimumRows, func(value *catalog.Catalog) error {
		var problem error
		report, problem = workflows.ComparePrepared(ctx, report, prepared, value.Compare)
		return problem
	})
	if err != nil {
		report.Complete, report.Equal = false, false
	}
	return report, err
}

func listCatalog(ctx context.Context, invocation *command.Invocation, resolver DatabaseResolver, request catalog.ListRequest) (catalog.Page, error) {
	var result catalog.Page
	if err := catalog.ValidateList(request); err != nil {
		return result, err
	}
	err := withCatalog(ctx, invocation, resolver, 0, 0, func(value *catalog.Catalog) error {
		var err error
		result, err = value.List(ctx, request)
		return err
	})
	if err != nil {
		return catalog.Page{}, err
	}
	return result, nil
}
func readCatalog(ctx context.Context, invocation *command.Invocation, resolver DatabaseResolver, request catalog.ReadRequest) (catalog.View, error) {
	var result catalog.View
	if err := catalog.ValidateRead(request); err != nil {
		return result, err
	}
	err := withCatalog(ctx, invocation, resolver, 0, 0, func(value *catalog.Catalog) error {
		var err error
		result, err = value.Read(ctx, request)
		return err
	})
	if err != nil {
		return catalog.View{}, err
	}
	return result, nil
}

// The invocation owns this finite operation; its Framework runtime supplies the
// existing resource scope. No separate database owner, pool or registry is created.
func buildControl[T controlOperation](ctx context.Context, selected databaseSelection, construct func(controlClients) (T, error)) (instance *resource.Instance[T], err error) {
	targetPolicy, err := databasePolicy(selected.target)
	if err != nil {
		return nil, err
	}
	options := targetPolicy.Runtime
	options.Name = "command-database"
	options.MaxActive++
	workBytes := selected.workBytes + 2*targetPolicy.Budget.EvidenceBytes
	var adminPolicy vocabulary.Policy
	if selected.create {
		adminPolicy, err = databasePolicy(selected.admin)
		if err != nil {
			return nil, err
		}
		options.MaxActive += adminPolicy.Runtime.MaxActive
		options.MaxWorkBytes += adminPolicy.Runtime.MaxWorkBytes
		options.MaxQueued += adminPolicy.Runtime.MaxQueued
		options.MaxQueuedBytes += adminPolicy.Runtime.MaxQueuedBytes
		workBytes += 2 * adminPolicy.Budget.EvidenceBytes
	}
	options.MaxWorkBytes += workBytes
	runtime, err := framework.New(ctx, framework.Options{Operations: options, Resources: resource.Options{
		Name: "command-database", MaxBindings: 2, MaxPreparing: 1, CleanupTimeout: selected.target.CleanupTimeout,
	}})
	if err != nil {
		return nil, err
	}
	var operation T
	var ready, settled bool
	var workspace *adapters.Call[struct{}]
	var guard adapters.Guard
	var operationError, constructionError, receivedError error
	var finishers []func(context.Context) error
	cleanup := make(chan error, 1)
	defer func() { constructionError = err }()
	instance = &resource.Instance[T]{Release: func(wait context.Context) resource.ReleaseResult {
		if ready {
			problem := operation.Close(wait)
			if !operation.Released() {
				return resource.ReleaseResult{Err: errors.Join(databasecmd.ErrCleanup, problem)}
			}
			operationError, ready = problem, false
		}
		if workspace != nil && !settled {
			if problem := workspace.Resolve(adapters.Outcome[struct{}]{Primary: constructionError, Cleanup: operationError}); problem != nil {
				return resource.ReleaseResult{Err: problem}
			}
			if problem := guard.Release(); problem != nil {
				return resource.ReleaseResult{Err: problem}
			}
			settled = true
		}
		runtimeError := runtime.Close(wait)
		status, inspected := runtime.Operations().Inspect()
		if inspected != nil || !status.Closed || !runtime.Resources().ShutdownComplete() {
			return resource.ReleaseResult{Err: errors.Join(databasecmd.ErrCleanup, operationError, runtimeError, inspected)}
		}
		for len(finishers) > 0 {
			if problem := finishers[0](wait); problem != nil {
				return resource.ReleaseResult{Err: errors.Join(operationError, runtimeError, problem)}
			}
			finishers = finishers[1:]
		}
		select {
		case problem := <-cleanup:
			receivedError = problem
		default:
		}
		return resource.ReleaseResult{Complete: true, Err: errors.Join(operationError, runtimeError, receivedError)}
	}}
	inbox, finish, err := controlReceiver[struct{}](ctx, adapters.EvidenceOptions{Capacity: 4, MaxBytes: 64 << 10}, cleanup)
	if err != nil {
		return instance, err
	}
	finishers = append(finishers, finish)
	endpoint, err := adapters.Bind(runtime.Operations(), adapters.Declaration[struct{}]{Evidence: inbox, Copy: func(value struct{}) struct{} { return value }})
	if err != nil {
		return instance, err
	}
	receipt, err := endpoint.Run(ctx, adapters.Request{Operation: selected.workspace, WorkBytes: workBytes, EvidenceBytes: 16 << 10}, func(call *adapters.Call[struct{}]) {
		guard, err = call.Hold()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[struct{}]{Primary: err})
			return
		}
		workspace = call
	})
	if err != nil {
		return instance, err
	}
	if workspace == nil {
		snapshot, _ := receipt.Snapshot()
		return instance, errors.Join(databasecmd.ErrLimit, snapshot.Err())
	}

	clients := controlClients{}
	if selected.target.PostgreSQL != nil {
		var providerInbox *adapters.Inbox[postgres.Result]
		providerInbox, finish, err = controlReceiver[postgres.Result](ctx, targetPolicy.Evidence, cleanup)
		if err != nil {
			return instance, err
		}
		finishers = append(finishers, finish)
		dependencies := postgres.Dependencies{Runtime: runtime.Operations(), Evidence: providerInbox}
		clients.pg, err = bindPostgres(ctx, runtime.Resources(), false, targetPolicy.Budget, dependencies)
		if err == nil && selected.create {
			providerInbox, finish, err = controlReceiver[postgres.Result](ctx, adminPolicy.Evidence, cleanup)
			if err != nil {
				return instance, err
			}
			finishers = append(finishers, finish)
			dependencies.Evidence = providerInbox
			clients.adminPG, err = bindPostgres(ctx, runtime.Resources(), true, adminPolicy.Budget, dependencies)
		}
	} else {
		var providerInbox *adapters.Inbox[mysql.Result]
		providerInbox, finish, err = controlReceiver[mysql.Result](ctx, targetPolicy.Evidence, cleanup)
		if err != nil {
			return instance, err
		}
		finishers = append(finishers, finish)
		dependencies := mysql.Dependencies{Runtime: runtime.Operations(), Evidence: providerInbox}
		clients.mysql, err = bindMySQL(ctx, runtime.Resources(), false, targetPolicy.Budget, dependencies)
		if err == nil && selected.create {
			providerInbox, finish, err = controlReceiver[mysql.Result](ctx, adminPolicy.Evidence, cleanup)
			if err != nil {
				return instance, err
			}
			finishers = append(finishers, finish)
			dependencies.Evidence = providerInbox
			clients.adminMySQL, err = bindMySQL(ctx, runtime.Resources(), true, adminPolicy.Budget, dependencies)
		}
	}
	if err != nil {
		return instance, err
	}
	snapshot, err := settings.New(selected, databaseSelection.copy)
	if err != nil {
		return instance, err
	}
	update, err := runtime.Resources().Apply(ctx, snapshot.View())
	if err != nil {
		return instance, err
	}
	if err = update.Wait(ctx); err != nil {
		return instance, err
	}
	operation, err = construct(clients)
	if err != nil {
		return instance, err
	}
	instance.Value, ready = operation, true
	return instance, nil
}

func controlReceiver[T any](ctx context.Context, options adapters.EvidenceOptions, cleanup chan<- error) (*adapters.Inbox[T], func(context.Context) error, error) {
	inbox, err := adapters.NewInbox[T](options)
	if err != nil {
		return nil, nil, err
	}
	receiver, err := framework.StartReceiver(context.WithoutCancel(ctx), inbox, framework.ReceiverOptions{}, func(_ context.Context, value adapters.Snapshot[T]) error {
		if problem := value.Cleanup(); problem != nil {
			select {
			case cleanup <- problem:
			default:
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return inbox, receiver.Finish, nil
}

func bindDatabase[T any](scope *resource.Scope, admin bool, build func(context.Context, DatabaseConfiguration) (*resource.Instance[T], error)) (resource.Ref[T], error) {
	name := "control-target"
	if admin {
		name = "control-admin"
	}
	return resource.Bind(scope, resource.Binding[DatabaseConfiguration, T]{
		Name: name, Policy: resource.Fixed, MaxBorrowers: 1, Clone: DatabaseConfiguration.copy,
		Select: func(view settings.View) (DatabaseConfiguration, error) {
			snapshot, err := settings.As[databaseSelection](view)
			if err != nil {
				return DatabaseConfiguration{}, err
			}
			value, err := snapshot.ValueCopy()
			if admin {
				return value.admin, err
			}
			return value.target, err
		}, Build: build,
	})
}
func bindPostgres(ctx context.Context, scope *resource.Scope, admin bool, budget vocabulary.Budget, dependencies postgres.Dependencies) (*postgres.Client, error) {
	ref, err := bindDatabase(scope, admin, func(lifetime context.Context, value DatabaseConfiguration) (*resource.Instance[postgres.Handle], error) {
		owner, err := postgres.Open(lifetime, *value.PostgreSQL, dependencies)
		if owner == nil {
			return nil, err
		}
		return &resource.Instance[postgres.Handle]{Value: owner.Handle(), Release: owner.Release}, err
	})
	if err != nil {
		return nil, err
	}
	return postgres.Using(ctx, ref, budget, dependencies)
}
func bindMySQL(ctx context.Context, scope *resource.Scope, admin bool, budget vocabulary.Budget, dependencies mysql.Dependencies) (*mysql.Client, error) {
	ref, err := bindDatabase(scope, admin, func(lifetime context.Context, value DatabaseConfiguration) (*resource.Instance[mysql.Handle], error) {
		owner, err := mysql.Open(lifetime, *value.MySQL, dependencies)
		if owner == nil {
			return nil, err
		}
		return &resource.Instance[mysql.Handle]{Value: owner.Handle(), Release: owner.Release}, err
	})
	if err != nil {
		return nil, err
	}
	return mysql.Using(ctx, ref, budget, dependencies)
}
func loadAdministration(ctx context.Context, path string) (administration, error) {
	var value administration
	if path == "" || len(path) > 4096 {
		return value, command.Fail(command.ErrUsage)
	}
	encoding := configuration.Encoding(strings.ToLower(strings.TrimPrefix(filepath.Ext(path), ".")))
	if encoding == "yml" {
		encoding = configuration.YAML
	}
	if encoding != configuration.JSON && encoding != configuration.YAML && encoding != configuration.TOML {
		return value, command.Fail(command.ErrUsage)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return value, err
	}
	provider, err := configuration.Viper(configuration.ViperOptions{Documents: []configuration.File{{Path: absolute, Kind: configuration.Base, Encoding: encoding}}})
	if err != nil {
		return value, err
	}
	loaded, err := configuration.Load(ctx, configuration.Declaration[administration]{Schema: configuration.Schema[administration]{
		Version: 1, Validate: func(_ context.Context, value administration) error {
			return validateAdministration(value)
		},
	}}, configuration.Dependencies{Provider: provider})
	if err != nil {
		return value, err
	}
	if loaded.State == nil || len(loaded.Records) > 128 {
		return value, databasecmd.ErrConfiguration
	}
	for _, record := range loaded.Records {
		if !record.Info().Released || !record.Info().Resolved || record.Err() != nil {
			return value, errors.Join(databasecmd.ErrConfiguration, record.Err())
		}
	}
	snapshot, err := loaded.State.Capture()
	if err != nil {
		return value, err
	}
	return snapshot.ValueCopy()
}
func withOwned[C, T any](ctx context.Context, invocation *command.Invocation, selected C, binding resource.Binding[C, T], run func(T) error) (result error) {
	scope, err := invocation.Scope()
	if err != nil {
		return controlFailure(err)
	}
	binding.Policy, binding.MaxBorrowers = resource.Fixed, 1
	binding.Select = func(view settings.View) (C, error) {
		value, err := settings.As[C](view)
		if err != nil {
			var empty C
			return empty, err
		}
		return value.ValueCopy()
	}
	ref, err := resource.Bind(scope, binding)
	if err != nil {
		return controlFailure(err)
	}
	snapshot, err := settings.New(selected, binding.Clone)
	if err != nil {
		return controlFailure(err)
	}
	update, err := scope.Apply(ctx, snapshot.View())
	if err != nil {
		return controlFailure(err)
	}
	if err = update.Wait(ctx); err != nil {
		return controlFailure(err)
	}
	lease, err := ref.Acquire(ctx)
	if err != nil {
		return controlFailure(err)
	}
	owner, err := lease.Value()
	if err != nil {
		return errors.Join(controlFailure(err), lease.Release())
	}
	defer func() {
		if cleanup := lease.Release(); cleanup != nil {
			result = command.Fail(command.ErrCleanup, result, cleanup)
		}
	}()
	return run(owner)
}
func controlFailure(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return command.Fail(command.ErrCanceled, err)
	}
	return command.Fail(command.ErrExecution, err)
}
