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
	"net"
	"sync/atomic"
	"time"

	temporal "github.com/frost-leo/fathomry/adapters/orchestration/temporal/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"
)

type peer struct {
	workflowservice.UnimplementedWorkflowServiceServer
	signals atomic.Int64
	address string
	stop    func()
}

func (server *peer) GetSystemInfo(context.Context, *workflowservice.GetSystemInfoRequest) (*workflowservice.GetSystemInfoResponse, error) {
	return &workflowservice.GetSystemInfoResponse{ServerVersion: "1.32.0"}, nil
}
func (server *peer) SignalWorkflowExecution(context.Context, *workflowservice.SignalWorkflowExecutionRequest) (*workflowservice.SignalWorkflowExecutionResponse, error) {
	server.signals.Add(1)
	return &workflowservice.SignalWorkflowExecutionResponse{}, nil
}
func newPeer() (*peer, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer()
	result := &peer{address: listener.Addr().String(), stop: server.Stop}
	workflowservice.RegisterWorkflowServiceServer(server, result)
	go server.Serve(listener)
	return result, nil
}
func prepare(name string, server *peer) (temporal.Prepared, error) {
	return temporal.Prepare(temporal.Settings{Name: name, Endpoint: server.address, Namespace: name, Plaintext: true, MaxActive: 2,
		InnerEvidenceCapacity: 16, MaxRequestBytes: 1024, MaxResponseBytes: 1024}, temporal.NativeOptions{})
}
func dependencies(runtime *adapters.Runtime, policy temporal.Policy) (temporal.Dependencies, error) {
	evidence, err := adapters.NewInbox[temporal.Result](policy.Evidence)
	if err != nil {
		return temporal.Dependencies{}, err
	}
	workers, err := adapters.NewInbox[temporal.WorkerResult](policy.Workers)
	if err != nil {
		return temporal.Dependencies{}, err
	}
	tasks, err := adapters.NewInbox[temporal.TaskResult](policy.Tasks)
	if err != nil {
		return temporal.Dependencies{}, err
	}
	return temporal.Dependencies{Runtime: runtime, Evidence: evidence, Workers: workers, Tasks: tasks}, nil
}
func finish(ctx context.Context, owner *temporal.Owner, deps temporal.Dependencies) error {
	if err := owner.Close(ctx); err != nil {
		return err
	}
	if !owner.ShutdownComplete() {
		return errors.New("physical shutdown unconfirmed")
	}
	for {
		status, err := deps.Evidence.Inspect()
		if err != nil {
			return err
		}
		if status.Outstanding == 0 {
			break
		}
		delivery, err := deps.Evidence.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
	}
	return nil
}
func bounded() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}
