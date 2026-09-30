//go:build resource_service

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

package conformance_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/internal/configsource/nacos/v2"
	pgx "github.com/frost-leo/fathomry/internal/database/pgx/v5"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	native "github.com/frost-leo/fathomry/internal/resource"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type resourceServiceFixture struct {
	AllowWrites bool `json:"allow_writes"`
	Nacos       struct {
		HTTPURL       string `json:"http_url"`
		GRPCAddress   string `json:"grpc_address"`
		Namespace     string `json:"namespace"`
		Username      string `json:"username"`
		Password      string `json:"password"`
		AdminUsername string `json:"admin_username"`
		AdminPassword string `json:"admin_password"`
		AllowInsecure bool   `json:"allow_insecure"`
		AllowWrites   bool   `json:"allow_writes"`
	} `json:"nacos"`
	Postgres struct {
		Address    string `json:"address"`
		Port       uint16 `json:"port"`
		User       string `json:"user"`
		Password   string `json:"password"`
		Database   string `json:"database"`
		RootCAPEM  string `json:"root_ca_pem"`
		ServerName string `json:"server_name"`
		Plaintext  bool   `json:"plaintext"`
	} `json:"postgres"`
}

func loadResourceServiceFixture(t *testing.T) resourceServiceFixture {
	t.Helper()
	file, err := os.Open(os.Getenv("FATHOMRY_RESOURCE_TEST_CONFIG"))
	if err != nil {
		t.Fatal("explicit private resource-service fixture unavailable")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 128<<10 {
		t.Fatal("private resource-service fixture permissions or bounds invalid")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 128<<10+1))
	decoder.DisallowUnknownFields()
	var fixture resourceServiceFixture
	if decoder.Decode(&fixture) != nil || !errors.Is(decoder.Decode(new(any)), io.EOF) || !fixture.AllowWrites || !fixture.Nacos.AllowWrites {
		t.Fatal("invalid fixture or absent isolated-write authorization")
	}
	return fixture
}

func serviceScope(t *testing.T, ctx context.Context) *resource.Scope {
	t.Helper()
	scope, err := resource.New(ctx, resource.Options{Name: "service-proof", CleanupTimeout: time.Second})
	if err != nil {
		t.Fatal("public scope creation failed")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if scope.Close(cleanup) != nil {
			t.Error("public scope cleanup did not confirm completion")
		}
	})
	return scope
}

func serviceNacos(t *testing.T, ctx context.Context, fixture resourceServiceFixture) (*nacos.Client, nacos.KeyV1) {
	t.Helper()
	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("test identity unavailable")
	}
	key := nacos.KeyV1{Group: "DEFAULT_GROUP", DataID: "gh98-resource-" + hex.EncodeToString(nonce[:]) + ".json"}
	options := nacos.OptionsV1{
		Name: "resource-service", Namespace: fixture.Nacos.Namespace, DynamicKeys: true, Writable: true,
		Servers:  []nacos.ServerV1{{HTTPURL: fixture.Nacos.HTTPURL, GRPCAddress: fixture.Nacos.GRPCAddress}},
		Username: fixture.Nacos.AdminUsername, Password: fixture.Nacos.AdminPassword,
		AllowInsecure: fixture.Nacos.AllowInsecure, ReconcileInterval: 5 * time.Minute, RequestTimeout: 5 * time.Second,
		ConcurrentRequests: 8, Subscriptions: 3,
	}
	client, err := nacos.Open(ctx, options)
	if err != nil {
		t.Fatal("native source creation failed")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if client.Close(cleanup) != nil {
			t.Error("native source cleanup incomplete")
		}
	})
	document, err := client.ReadRaw(ctx, key)
	if err != nil || !document.Missing() {
		t.Fatal("unique test key absence not established")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = client.Delete(cleanup, key)
		for cleanup.Err() == nil {
			document, err := client.ReadRaw(cleanup, key)
			if err == nil && document.Missing() {
				t.Log("Isolated Nacos resource fixture deletion and absence confirmed.")
				return
			}
			select {
			case <-time.After(100 * time.Millisecond):
			case <-cleanup.Done():
			}
		}
		t.Errorf("owned Nacos fixture cleanup incomplete: %s", key.DataID)
	})
	return client, key
}

func serviceWatch(t *testing.T, ctx context.Context, client *nacos.Client, key nacos.KeyV1) *nacos.Subscription {
	t.Helper()
	subscription, err := client.WatchKeys(ctx, []nacos.KeyV1{key})
	if err != nil {
		t.Fatal("native observation creation failed")
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if subscription.Close(cleanup) != nil {
			t.Error("native observation cleanup incomplete")
		}
	})
	wait, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	change, err := subscription.Next(wait)
	if err != nil || change.Err() != nil || !change.Resync() {
		t.Fatal("native registration readiness absent")
	}
	return subscription
}

func servicePublish(t *testing.T, ctx context.Context, client *nacos.Client, key nacos.KeyV1, content string) {
	t.Helper()
	result, err := client.Publish(ctx, nacos.PublishInputV1{Key: key, Content: content, ContentType: "json"})
	if err != nil || result.State() != nacos.MutationAcknowledged {
		t.Fatal("isolated publication not acknowledged")
	}
}

func serviceObserved(t *testing.T, ctx context.Context, client *nacos.Client, subscription *nacos.Subscription, key nacos.KeyV1, content string) []byte {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		change, err := subscription.Next(wait)
		if err != nil || change.Err() != nil {
			t.Fatal("native change notification failed")
		}
		if !change.Resync() && change.Key().DataID == key.DataID {
			break
		}
	}
	for wait.Err() == nil {
		document, err := client.ReadRaw(wait, key)
		if err == nil && !document.Missing() && string(document.RawCopy()) == content {
			return document.RawCopy()
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-wait.Done():
		}
	}
	t.Fatal("notified document did not reach the expected fixture content")
	return nil
}

type serviceConfiguration struct {
	PoolSize        int  `json:"pool_size"`
	RejectReadiness bool `json:"reject_readiness"`
}

type serviceDatabase struct {
	database *pgx.Database
	assembly *native.Assembly
	inbox    *invocation.Inbox[pgx.Result]
}

// This bridge belongs only to maintainer tests. Public resource has no Internal dependency.
func buildServiceDatabase(ctx context.Context, fixture resourceServiceFixture, config serviceConfiguration, name string) (*resource.Instance[*serviceDatabase], error) {
	options := pgx.OptionsV1{
		Name: name, Address: fixture.Postgres.Address, Port: fixture.Postgres.Port,
		User: fixture.Postgres.User, Password: fixture.Postgres.Password, Database: fixture.Postgres.Database,
		RootCAPEM: fixture.Postgres.RootCAPEM, ServerName: fixture.Postgres.ServerName, Plaintext: fixture.Postgres.Plaintext,
		ParserHome: os.Getenv("HOME"), MaxConnections: config.PoolSize,
		MaxRows: 8, MaxResultBytes: 4096, Timeout: 5 * time.Second, CloseTimeout: time.Second,
	}
	selected, err := pgx.Select(options)
	if err != nil {
		return nil, err
	}
	selected = native.WithLimits(selected, pgx.LimitsV1(options))
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	assembly, err := native.Assemble(ctx, cleanup, name, selected)
	cancel()
	if assembly == nil {
		return nil, err
	}
	value := &serviceDatabase{assembly: assembly}
	instance := &resource.Instance[*serviceDatabase]{Value: value, Release: value.close}
	if err != nil {
		return instance, err
	}
	value.inbox, err = invocation.NewInbox[pgx.Result](64, 256<<20)
	if err != nil {
		return instance, err
	}
	value.database, err = pgx.Bind(assembly, selected, value.inbox, nil)
	if err != nil {
		return instance, err
	}
	query := "SELECT 1"
	if config.RejectReadiness {
		query = "SELECT 1/0"
	}
	receipt, err := value.database.Query(ctx, fault.Correlation{Call: "readiness"}, query)
	if err != nil {
		return instance, err
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil {
		return instance, err
	}
	return instance, result.Err()
}

func (value *serviceDatabase) close(ctx context.Context) resource.ReleaseResult {
	err := value.assembly.Close(ctx)
	complete := true
	for _, source := range value.assembly.Snapshot().Sources {
		if source.Pending {
			complete = false
		}
	}
	if value.inbox != nil {
		for value.inbox.Usage().Outstanding > 0 {
			record, receiveErr := value.inbox.Next(ctx)
			if receiveErr != nil {
				return resource.ReleaseResult{Err: errors.Join(err, receiveErr)}
			}
			if _, waitErr := record.Receipt().WaitReleased(ctx); waitErr != nil {
				return resource.ReleaseResult{Err: errors.Join(err, waitErr)}
			}
			if releaseErr := record.Release(); releaseErr != nil {
				return resource.ReleaseResult{Err: errors.Join(err, releaseErr)}
			}
		}
	}
	return resource.ReleaseResult{Complete: complete, Err: err}
}

func serviceResult(t *testing.T, receipt *invocation.Receipt[pgx.Result], err error) pgx.Result {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("native operation admission failed")
	}
	result, ok := receipt.Result()
	if !ok || !result.Final || result.Err() != nil || !result.Outcome.Present {
		t.Fatal("native operation did not complete successfully")
	}
	return result.Outcome.Value
}

func serviceCell(t *testing.T, value pgx.Result) string {
	t.Helper()
	row, err := value.First()
	if err != nil || len(row.ValuesCopy()) != 1 {
		t.Fatal("single-cell service result absent")
	}
	return string(row.ValuesCopy()[0])
}

func TestResourceService(t *testing.T) {
	fixture := loadResourceServiceFixture(t)
	t.Run("nacos_watch_acceptance_pool_replacement_and_old_transaction", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		t.Cleanup(cancel)
		client, key := serviceNacos(t, ctx, fixture)
		subscription := serviceWatch(t, ctx, client, key)
		scope := serviceScope(t, ctx)
		var serial atomic.Uint64
		binding := func(name string, policy resource.Policy) resource.Binding[serviceConfiguration, *serviceDatabase] {
			return resource.Binding[serviceConfiguration, *serviceDatabase]{
				Name: name, Policy: policy,
				Select: func(view settings.View) (serviceConfiguration, error) {
					snapshot, err := settings.As[serviceConfiguration](view)
					if err != nil {
						return serviceConfiguration{}, err
					}
					return snapshot.ValueCopy()
				},
				Clone: func(value serviceConfiguration) serviceConfiguration { return value },
				Equal: func(left, right serviceConfiguration) bool { return left == right },
				Build: func(ctx context.Context, value serviceConfiguration) (*resource.Instance[*serviceDatabase], error) {
					return buildServiceDatabase(ctx, fixture, value, fmt.Sprintf("%s-%d", name, serial.Add(1)))
				},
			}
		}
		follow, err := resource.Bind(scope, binding("follow", resource.Follow))
		if err != nil {
			t.Fatal("following binding failed")
		}
		fixed, err := resource.Bind(scope, binding("fixed", resource.Fixed))
		if err != nil {
			t.Fatal("fixed binding failed")
		}
		views := make(chan settings.View, 1)
		watch, err := scope.Watch(ctx, views)
		if err != nil {
			t.Fatal("accepted-view receiver failed")
		}
		store := settings.NewStore[serviceConfiguration]()
		apply := func(content string, rejected bool) *resource.Update {
			t.Helper()
			servicePublish(t, ctx, client, key, content)
			raw := serviceObserved(t, ctx, client, subscription, key, content)
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			decoder.DisallowUnknownFields()
			var value serviceConfiguration
			problem := decoder.Decode(&value)
			valid := problem == nil && errors.Is(decoder.Decode(new(any)), io.EOF) && value.PoolSize >= 1 && value.PoolSize <= 4
			if rejected {
				if valid {
					t.Fatal("invalid source data was accepted")
				}
				return nil
			}
			if !valid {
				t.Fatal("valid source configuration rejected")
			}
			snapshot, err := settings.New(value, func(value serviceConfiguration) serviceConfiguration { return value })
			if err != nil || store.Publish(snapshot) != nil {
				t.Fatal("accepted settings publication failed")
			}
			views <- snapshot.View()
			report, err := watch.Next(ctx)
			if err != nil || report.Err != nil {
				t.Fatal("accepted-view handoff failed")
			}
			return report.Update
		}
		first := apply(`{"pool_size":2}`, false)
		if err := first.Wait(ctx); err != nil {
			t.Fatal("initial native pool readiness failed", err)
		}
		fixedBefore, _ := fixed.Inspect()
		old, err := follow.Acquire(ctx)
		if err != nil {
			t.Fatal("old pool borrow failed")
		}
		t.Cleanup(func() { _ = old.Release() })
		before, _ := old.Value()
		transaction, _, err := before.database.Begin(ctx, fault.Correlation{Call: "old-transaction"}, pgx.TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
		if err != nil || transaction == nil {
			t.Fatal("native transaction startup failed")
		}
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			_, _ = transaction.Rollback(cleanup)
		})
		query := func(sql string, args ...any) pgx.Result {
			t.Helper()
			receipt, err := transaction.Query(ctx, fault.Correlation{Call: "old-query"}, sql, args...)
			return serviceResult(t, receipt, err)
		}
		oldPID := serviceCell(t, query("SELECT pg_backend_pid()"))
		table := "resource_" + strings.TrimSuffix(strings.TrimPrefix(key.DataID, "gh98-resource-"), ".json")
		receipt, err := transaction.Exec(ctx, fault.Correlation{Call: "create-temp"}, "CREATE TEMP TABLE "+table+" (value bigint) ON COMMIT DROP")
		serviceResult(t, receipt, err)
		receipt, err = transaction.Exec(ctx, fault.Correlation{Call: "insert-temp"}, "INSERT INTO "+table+" VALUES ($1)", int64(42))
		serviceResult(t, receipt, err)

		accepted, _ := store.Reader().Capture()
		apply("{", true)
		apply(`{"pool_size":0}`, true)
		unchanged, _ := store.Reader().Capture()
		if accepted != unchanged {
			t.Fatal("rejected source data replaced accepted settings")
		}

		failed := apply(`{"pool_size":3,"reject_readiness":true}`, false)
		var nativeFailure *pgconn.PgError
		if err := failed.Wait(ctx); !errors.Is(err, resource.ErrBuild) || !errors.As(err, &nativeFailure) || nativeFailure.Code != "22012" {
			t.Fatal("native readiness failure or public construction classification lost")
		}
		lastGood, _ := follow.Inspect()
		if lastGood.Generation != old.Generation() || !lastGood.Active {
			t.Fatal("failed replacement lost the old pool")
		}
		replacement := apply(`{"pool_size":4}`, false)
		if err := replacement.Wait(ctx); err != nil {
			t.Fatal("native replacement failed")
		}
		current, err := follow.Acquire(ctx)
		if err != nil {
			t.Fatal("new pool borrow failed")
		}
		t.Cleanup(func() { _ = current.Release() })
		after, _ := current.Value()
		receipt, err = after.database.Query(ctx, fault.Correlation{Call: "new-pid"}, "SELECT pg_backend_pid()")
		newPID := serviceCell(t, serviceResult(t, receipt, err))
		if newPID == oldPID || current.Generation() == old.Generation() {
			t.Fatal("replacement reused the old transaction connection")
		}
		if serviceCell(t, query("SELECT value FROM "+table)) != "42" || serviceCell(t, query("SELECT pg_backend_pid()")) != oldPID {
			t.Fatal("borrowed transaction moved or lost session state")
		}
		receipt, err = after.database.Query(ctx, fault.Correlation{Call: "no-temp"}, "SELECT to_regclass($1) IS NULL", "pg_temp."+table)
		if serviceCell(t, serviceResult(t, receipt, err)) != "t" {
			t.Fatal("new generation inherited old session state")
		}
		fixedAfter, _ := fixed.Inspect()
		if fixedAfter.Generation != fixedBefore.Generation {
			t.Fatal("fixed binding followed configuration")
		}
		if err := current.Release(); err != nil {
			t.Fatal("new borrow release failed")
		}

		wait, stop := context.WithTimeout(ctx, 25*time.Millisecond)
		err = scope.Close(wait)
		stop()
		if !errors.Is(err, resource.ErrWait) {
			t.Fatal("scope close abandoned a borrowed native transaction")
		}
		if serviceCell(t, query("SELECT value FROM "+table)) != "42" {
			t.Fatal("shutdown canceled a borrowed transaction")
		}
		receipt, err = transaction.Commit(ctx)
		if serviceResult(t, receipt, err).TransactionOutcome() != pgx.CommitAcknowledged {
			t.Fatal("old transaction commit not acknowledged")
		}
		receipt, err = before.database.Query(ctx, fault.Correlation{Call: "temp-removed"}, "SELECT to_regclass($1) IS NULL AND pg_backend_pid()::text=$2::text", "pg_temp."+table, oldPID)
		if serviceCell(t, serviceResult(t, receipt, err)) != "t" {
			t.Fatal("temporary fixture survived commit")
		}
		if err := old.Release(); err != nil {
			t.Fatal("old borrow release failed")
		}
		if err := scope.Close(ctx); err != nil {
			t.Fatal("native pool cleanup did not complete")
		}
		if err := first.Wait(ctx); err != nil {
			t.Fatal("shutdown rewrote historical adoption")
		}
		t.Log("Native push, rejected settings, partial readiness failure, Fixed/Follow, retained transaction, replacement and cleanup passed.")
	})

	t.Run("native_subscription_is_pinned_to_its_client_generation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		t.Cleanup(cancel)
		writer, key := serviceNacos(t, ctx, fixture)
		scope := serviceScope(t, ctx)
		ref, err := resource.Bind(scope, resource.Binding[int, *nacos.Client]{
			Name: "source", Policy: resource.Follow,
			Select: func(view settings.View) (int, error) {
				snapshot, err := settings.As[int](view)
				if err != nil {
					return 0, err
				}
				return snapshot.ValueCopy()
			},
			Clone: func(value int) int { return value },
			Equal: func(left, right int) bool { return left == right },
			Build: func(ctx context.Context, size int) (*resource.Instance[*nacos.Client], error) {
				client, err := nacos.Open(ctx, nacos.OptionsV1{
					Name: "leased-source", Namespace: fixture.Nacos.Namespace, DynamicKeys: true,
					Servers:  []nacos.ServerV1{{HTTPURL: fixture.Nacos.HTTPURL, GRPCAddress: fixture.Nacos.GRPCAddress}},
					Username: fixture.Nacos.AdminUsername, Password: fixture.Nacos.AdminPassword,
					AllowInsecure: fixture.Nacos.AllowInsecure, QueueCapacity: size, ReconcileInterval: 5 * time.Minute,
				})
				if client == nil {
					return nil, err
				}
				return &resource.Instance[*nacos.Client]{Value: client, Release: func(ctx context.Context) resource.ReleaseResult {
					err := client.Close(ctx)
					return resource.ReleaseResult{Complete: err == nil, Err: err}
				}}, err
			},
		})
		if err != nil {
			t.Fatal("source binding failed")
		}
		apply := func(size int) {
			t.Helper()
			snapshot, _ := settings.New(size, func(value int) int { return value })
			update, err := scope.Apply(ctx, snapshot.View())
			if err != nil || update.Wait(ctx) != nil {
				t.Fatal("source generation adoption failed")
			}
		}
		apply(2)
		old, err := ref.Acquire(ctx)
		if err != nil {
			t.Fatal("old source borrow failed")
		}
		t.Cleanup(func() { _ = old.Release() })
		oldClient, _ := old.Value()
		oldWatch := serviceWatch(t, ctx, oldClient, key)
		apply(4)
		current, err := ref.Acquire(ctx)
		if err != nil {
			t.Fatal("new source borrow failed")
		}
		t.Cleanup(func() { _ = current.Release() })
		newClient, _ := current.Value()
		newWatch := serviceWatch(t, ctx, newClient, key)
		servicePublish(t, ctx, writer, key, `{"value":1}`)
		serviceObserved(t, ctx, oldClient, oldWatch, key, `{"value":1}`)
		serviceObserved(t, ctx, newClient, newWatch, key, `{"value":1}`)
		if old.Generation() == current.Generation() {
			t.Fatal("source generation did not change")
		}
		if oldWatch.Close(ctx) != nil || old.Release() != nil {
			t.Fatal("old subscription retirement failed")
		}
		wait, stop := context.WithTimeout(ctx, 25*time.Millisecond)
		err = scope.Close(wait)
		stop()
		if !errors.Is(err, resource.ErrWait) {
			t.Fatal("live native subscription borrow was abandoned")
		}
		servicePublish(t, ctx, writer, key, `{"value":2}`)
		serviceObserved(t, ctx, newClient, newWatch, key, `{"value":2}`)
		if newWatch.Close(ctx) != nil || current.Release() != nil || scope.Close(ctx) != nil {
			t.Fatal("native subscription/client cleanup did not finish")
		}
		t.Log("Old/new native subscriptions retained distinct lifetimes; no automatic subscription migration was inferred.")
	})
}
