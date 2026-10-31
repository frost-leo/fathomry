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

package configuration

import (
	"context"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

// LoadConfiguration loads and validates the first complete main configuration.
// A nil error means the returned Session has an immutable initial View. When
// bootstrap.Configuration.Watch is true the same Session continues observing
// updates; it does not start application services or install global settings.
//
// Bootstrap and captured variables are detached before acquisition. Schema's
// validator is borrowed, pure, bounded and valid until Session shutdown. Defaults
// need not be complete; semantic validation runs after all selected layers merge.
//
// A non-nil Session must be closed even when an error is returned: cancellation
// can precede actual producer cleanup. Dependencies remain caller-owned, and their
// required evidence is not acknowledged here. lifetime owns the producer, while
// bootstrap's initial/candidate budgets bound acceptance.
func LoadConfiguration[T any](
	lifetime context.Context,
	bootstrap Bootstrap,
	schema Schema[T],
	variables []Variable,
	dependencies Dependencies,
) (*Session[T], error) {
	if lifetime == nil {
		return nil, at(ErrDeclaration, "load_configuration", -1)
	}
	if err := validateBootstrapBudgets(bootstrap); err != nil {
		return nil, err
	}
	initial, cancel := context.WithTimeout(lifetime, bootstrap.InitialTimeout)
	defer cancel()
	return loadConfiguration(lifetime, initial, bootstrap, schema, variables, dependencies, nil)
}

func loadConfiguration[T any](lifetime, initial context.Context, bootstrap Bootstrap, schema Schema[T], variables []Variable, dependencies Dependencies, accepted *settings.Store[Loaded]) (*Session[T], error) {
	if err := validateBootstrapBudgets(bootstrap); err != nil {
		return nil, err
	}
	session, err := startSessionBefore(lifetime, initial, bootstrap.Configuration, schema, sessionOptions{
		CandidateTimeout: bootstrap.CandidateTimeout,
		ShutdownTimeout:  bootstrap.ShutdownTimeout,
		Variables:        variables,
		accepted:         accepted,
	}, dependencies)
	if err != nil {
		return session, err
	}
	return session, session.waitInitial(initial)
}

func validateBootstrapBudgets(value Bootstrap) error {
	if value.InitialTimeout < time.Millisecond || value.InitialTimeout > 5*time.Minute ||
		value.CandidateTimeout < time.Millisecond || value.CandidateTimeout > 5*time.Minute ||
		value.ShutdownTimeout < time.Millisecond || value.ShutdownTimeout > time.Minute {
		return at(ErrDeclaration, "bootstrap.budgets", -1)
	}
	return nil
}

type sessionOptions struct {
	CandidateTimeout time.Duration
	ShutdownTimeout  time.Duration
	Variables        []Variable
	accepted         *settings.Store[Loaded]
}

func startSessionBefore[T any](lifetime, initial context.Context, plan SourcePlan, schema Schema[T], options sessionOptions, dependencies Dependencies) (*Session[T], error) {
	if lifetime == nil || initial == nil || schema.Version == 0 || schema.Validate == nil || options.CandidateTimeout <= 0 {
		return nil, at(ErrDeclaration, "start", -1)
	}
	if err := lifetime.Err(); err != nil {
		return nil, at(ErrClosed, "start", -1, err, context.Cause(lifetime))
	}
	deadline, present := initial.Deadline()
	if !present {
		return nil, at(ErrDeclaration, "application.deadline", -1)
	}
	if err := checkInitialDeadline(initial, deadline); err != nil {
		return nil, err
	}
	selected, err := preparePlan(initial, plan)
	if err != nil {
		return nil, err
	}
	defaults, err := configsource.Prepare(initial, configsource.Schema[T]{Version: schema.Version, Defaults: schema.Defaults}, nil)
	if err != nil {
		return nil, err
	}
	schema.Defaults, err = defaults.ValueCopy()
	if err != nil {
		return nil, err
	}
	if len(options.Variables) > configsource.MaxVariables {
		return nil, at(ErrLimit, "configuration.variables", -1)
	}
	bindings := make([]configsource.Variable, len(options.Variables))
	for index, variable := range options.Variables {
		bindings[index] = configsource.Variable{Path: variable.Path, Value: variable.Value, Present: variable.Present, JSON: variable.JSON}
	}
	variables, err := configsource.BindVariables[T](bindings)
	if err != nil {
		return nil, err
	}
	if err := providerBudget(selected, dependencies); err != nil {
		return nil, err
	}
	if err := checkInitialDeadline(initial, deadline); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancelCause(lifetime)
	state := &sessionState[T]{
		ctx: ctx, cancel: cancel, plan: selected,
		schema:    configsource.Schema[T]{Version: schema.Version, Defaults: schema.Defaults, Validate: schema.Validate},
		variables: variables, haveVariables: len(options.Variables) != 0,
		options:      sessionOptions{CandidateTimeout: options.CandidateTimeout, ShutdownTimeout: options.ShutdownTimeout, accepted: options.accepted},
		dependencies: dependencies, store: settings.NewStore[T](), phase: Initializing, stage: Acquisition,
		initialDone: make(chan struct{}), workDone: make(chan struct{}), workerDone: make(chan struct{}), guardDone: make(chan struct{}),
		cleanupGate: make(chan struct{}, 1), changed: make(chan struct{}),
	}
	state.cleanupGate <- struct{}{}
	state.initialDeadline = deadline
	timer := time.NewTimer(time.Until(deadline))
	session := &Session[T]{state: state}
	go state.guard(timer)
	go state.run()
	return session, nil
}

func checkInitialDeadline(ctx context.Context, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return at(ErrWait, "application.initialize", -1, err, context.Cause(ctx))
	}
	if !time.Now().Before(deadline) {
		return at(ErrWait, "application.initialize", -1, context.DeadlineExceeded)
	}
	return nil
}
