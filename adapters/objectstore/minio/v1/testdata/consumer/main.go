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
	"os"
	"strings"
	"time"

	"github.com/frost-leo/fathomry/adapters/objectstore/minio/v1"
	"github.com/frost-leo/fathomry/adapters/objectstore/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	value := minio.Settings{Name: "external", Endpoint: os.Getenv("FATHOMRY_CONSUMER_ENDPOINT"), Plaintext: true, Region: "us-east-1", Bucket: "fixture", Prefix: "owned/", AccessKey: "fixture-access", SecretKey: "fixture-secret", Writes: true}
	policy, err := minio.Recommend(value)
	if err != nil {
		return err
	}
	policy, err = policy.ForGenerations(2)
	if err != nil {
		return err
	}
	host, err := framework.New(ctx, framework.Options{Operations: policy.Runtime})
	if err != nil {
		return err
	}
	defer host.Close(context.Background())
	inbox, err := adapters.NewInbox[minio.Result](policy.Evidence)
	if err != nil {
		return err
	}
	dependencies := minio.Dependencies{Runtime: host.Operations(), Evidence: inbox}
	owner, err := minio.Open(ctx, value, dependencies)
	if owner != nil {
		defer owner.Close(context.Background())
	}
	if err != nil {
		return err
	}
	await := func(receipt *adapters.Receipt[minio.Result], err error) (minio.Result, error) {
		if err != nil {
			return minio.Result{}, err
		}
		snapshot, err := receipt.WaitReleased(ctx)
		if err != nil {
			return minio.Result{}, err
		}
		result, _ := snapshot.ValueCopy()
		return result, snapshot.Err()
	}
	result, err := await(owner.Client().Put(ctx, ctx, minio.WriteRequest{Key: "owned/external", Size: 6}, strings.NewReader("direct")))
	if err != nil || result.Transfer().Effect != objectstore.Acknowledged {
		return errors.New("direct write did not acknowledge")
	}
	var metadata objectstore.Info = result.Source()
	if metadata.Revision == "" {
		return errors.New("source evidence missing")
	}
	ref, err := resource.Bind(host.Resources(), resource.Binding[minio.Settings, minio.Handle]{
		Name: "fixed", Policy: resource.Fixed,
		Select: func(view settings.View) (minio.Settings, error) {
			snapshot, err := settings.As[minio.Settings](view)
			if err != nil {
				return minio.Settings{}, err
			}
			return snapshot.ValueCopy()
		},
		Clone: func(value minio.Settings) minio.Settings { return value },
		Build: func(ctx context.Context, value minio.Settings) (*resource.Instance[minio.Handle], error) {
			owner, err := minio.Open(ctx, value, dependencies)
			if owner == nil {
				return nil, err
			}
			return &resource.Instance[minio.Handle]{Value: owner.Handle(), Release: owner.Release}, err
		},
	})
	if err != nil {
		return err
	}
	snapshot, err := settings.New(value, func(value minio.Settings) minio.Settings { return value })
	if err != nil {
		return err
	}
	update, err := host.Resources().Apply(ctx, snapshot.View())
	if err != nil {
		return err
	}
	if err := update.Wait(ctx); err != nil {
		return err
	}
	client, err := minio.Using(ctx, ref, policy.Budget, dependencies)
	if err != nil {
		return err
	}
	read, err := await(client.Read(ctx, minio.ReadRequest{Address: minio.Address{Key: "owned/external"}}))
	if err != nil || string(read.DataCopy()) != "direct" || read.Attribution().Source.Generation != 1 {
		return errors.New("Fixed native read failed")
	}
	_, err = await(client.Put(ctx, ctx, minio.WriteRequest{Key: "owned/external-fixed", Size: 5}, strings.NewReader("fixed")))
	if err != nil {
		return err
	}
	if err := owner.Close(ctx); err != nil {
		return err
	}
	if err := host.Close(ctx); err != nil {
		return err
	}
	for state, _ := inbox.Inspect(); state.Outstanding > 0; state, _ = inbox.Inspect() {
		record, err := inbox.NextReleased(ctx)
		if err != nil {
			return err
		}
		if err := record.Ack(); err != nil {
			return err
		}
	}
	fmt.Println("objectstore direct and Fixed consumers passed")
	return nil
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "consumer failed")
		os.Exit(1)
	}
}
