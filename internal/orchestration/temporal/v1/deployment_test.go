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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/frost-leo/fathomry/internal/orchestration/temporal/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdk "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
)

func TestReviewCallbackDeploymentMetadataAfterRelease(t *testing.T) {
	for _, withInterceptor := range []bool{false, true} {
		name := "without-interceptor"
		runtime := temporal.RuntimeOptions{}
		if withInterceptor {
			name = "with-interceptor"
			runtime.Interceptors = []interceptor.ClientInterceptor{&interceptor.ClientInterceptorBase{}}
		}
		t.Run(name, func(t *testing.T) {
			var calls, updates atomic.Int32
			var borrowed sdk.WorkerDeploymentHandle
			options := sdk.WorkerDeploymentUpdateVersionMetadataOptions{
				Version:        worker.WorkerDeploymentVersion{DeploymentName: "deployment", BuildID: "build"},
				MetadataUpdate: sdk.WorkerDeploymentMetadataUpdate{UpsertEntries: map[string]any{"fixture": activityMarshalProbe{called: &calls}}},
			}
			reviewReleasedCallbackFixture(t, "UpdateWorkerDeploymentVersionMetadata", runtime, func(ctx context.Context, client sdk.Client) error {
				borrowed = client.WorkerDeploymentClient().GetHandle("deployment")
				_, err := borrowed.UpdateVersionMetadata(ctx, options)
				return err
			}, func(ctx context.Context, request any, next grpc.UnaryHandler) (any, error) {
				if _, ok := request.(*workflowservice.UpdateWorkerDeploymentVersionMetadataRequest); ok {
					updates.Add(1)
					return &workflowservice.UpdateWorkerDeploymentVersionMetadataResponse{}, nil
				}
				return next(ctx, request)
			})
			if calls.Load() != 1 || updates.Load() != 1 {
				t.Fatalf("live control: conversions=%d updates=%d, want one each", calls.Load(), updates.Load())
			}
			_, err := borrowed.UpdateVersionMetadata(context.Background(), options)
			if !errors.Is(err, temporal.ErrAuthority) || calls.Load() != 1 || updates.Load() != 1 {
				t.Fatalf("released deployment handle: authority refusal=%v conversions=%d updates=%d; want refusal, one conversion, one update", errors.Is(err, temporal.ErrAuthority), calls.Load(), updates.Load())
			}
		})
	}
}
