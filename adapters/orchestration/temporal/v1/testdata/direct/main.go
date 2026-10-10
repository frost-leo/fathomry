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

package main

import (
	"context"
	"errors"
	"fmt"
	temporal "github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdk "go.temporal.io/sdk/client"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("temporal direct consumer passed")
}
func run() error {
	ctx, cancel := bounded()
	defer cancel()
	server, err := newPeer()
	if err != nil {
		return err
	}
	defer server.stop()
	prepared, err := prepare("direct", server)
	if err != nil {
		return err
	}
	policy, err := prepared.Policy()
	if err != nil {
		return err
	}
	runtime, err := adapters.New(ctx, policy.Runtime)
	if err != nil {
		return err
	}
	defer runtime.Close(context.Background())
	deps, err := dependencies(runtime, policy)
	if err != nil {
		return err
	}
	owner, err := prepared.Open(ctx, deps)
	if err != nil {
		return err
	}
	defer owner.Close(context.Background())
	first, err := owner.Client().Borrow(ctx)
	if err != nil {
		return err
	}
	peer, err := owner.Client().Borrow(ctx)
	if err != nil {
		return err
	}
	defer peer.Close(context.Background())
	intent, err := first.NewWithStartWorkflowOperation(sdk.StartWorkflowOptions{ID: "identity", TaskQueue: "queue", WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING}, "workflow")
	if err != nil {
		return err
	}
	if err := first.Close(ctx); err != nil {
		return err
	}
	if _, err := peer.UpdateWithStartWorkflow(ctx, intent, sdk.UpdateWorkflowOptions{UpdateName: "update", WaitForStage: sdk.WorkflowUpdateStageCompleted}); !errors.Is(err, temporal.ErrAuthority) {
		return errors.New("retained origin bypassed")
	}
	if err := peer.SignalWorkflow(ctx, "workflow", "run", "signal", "payload"); err != nil {
		return err
	}
	if server.signals.Load() != 1 {
		return errors.New("wrong native call count")
	}
	if err := peer.Close(ctx); err != nil {
		return err
	}
	if err := finish(ctx, owner, deps); err != nil {
		return err
	}
	return runtime.Close(ctx)
}
