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
	"net/netip"
	"os"
	"sync/atomic"
	"testing"
	"time"

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	framework "github.com/frost-leo/fathomry/framework/v1"
	pgx "github.com/frost-leo/fathomry/internal/database/pgx/v5"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type configurationServiceDatabase struct {
	Address         string `json:"address"`
	Port            uint16 `json:"port"`
	User            string `json:"user"`
	Password        string `json:"password"`
	Database        string `json:"database"`
	RootCAPEM       string `json:"root_ca_pem"`
	ServerName      string `json:"server_name"`
	Plaintext       bool   `json:"plaintext"`
	PoolSize        int    `json:"pool_size"`
	RejectReadiness bool   `json:"reject_readiness"`
}

type configurationServiceSettings struct {
	Version  int                          `json:"version"`
	Label    string                       `json:"label"`
	Database configurationServiceDatabase `json:"database"`
}

var errConfigurationServiceSchema = errors.New("invalid isolated application settings")

func configurationServiceSchema() configsource.Schema[configurationServiceSettings] {
	return configsource.Schema[configurationServiceSettings]{
		Version: 1,
		Validate: func(_ context.Context, value configurationServiceSettings) error {
			database := value.Database
			address, err := netip.ParseAddr(database.Address)
			if value.Version < 1 || err != nil || address.IsUnspecified() || database.Port == 0 ||
				database.User == "" || database.Database == "" || database.Password == "" ||
				database.PoolSize < 1 || database.PoolSize > 4 {
				return errConfigurationServiceSchema
			}
			return nil
		},
	}
}

func configurationServiceInbox[T any](t *testing.T) *adapters.Inbox[T] {
	t.Helper()
	inbox, err := adapters.NewInbox[T](adapters.EvidenceOptions{Capacity: 128, MaxBytes: 16 << 20})
	if err != nil {
		t.Fatal("service evidence inbox creation failed")
	}
	receiver, err := framework.StartReceiver(context.Background(), inbox, framework.ReceiverOptions{},
		func(context.Context, adapters.Snapshot[T]) error { return nil })
	if err != nil {
		t.Fatal("service evidence receiver creation failed")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if receiver.Finish(ctx) != nil {
			t.Error("released evidence did not drain after producer cleanup")
		}
		status, err := inbox.Inspect()
		if err != nil || status.Outstanding != 0 {
			t.Error("service evidence custody remains outstanding")
		}
		_ = receiver.Close(ctx)
	})
	return inbox
}

func configurationServiceRuntime(t *testing.T, ctx context.Context) (*framework.Runtime, nacos.Dependencies, configuration.Dependencies) {
	t.Helper()
	nativeInbox := configurationServiceInbox[nacos.Evidence](t)
	configInbox := configurationServiceInbox[configuration.Evidence](t)
	runtime, err := framework.New(ctx, framework.Options{
		Operations: adapters.Options{MaxWorkBytes: 256 << 20},
		Resources:  resource.Options{Name: "configuration-service", CleanupTimeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal("service framework creation failed")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if runtime.Close(ctx) != nil {
			t.Error("service framework shutdown incomplete")
		}
	})
	return runtime, nacos.Dependencies{Runtime: runtime.Operations(), Evidence: nativeInbox},
		configuration.Dependencies{Runtime: runtime.Operations(), Evidence: configInbox}
}

func configurationServiceBootstrap(t *testing.T, ctx context.Context, deps configuration.Dependencies, writable bool) nacos.Settings {
	t.Helper()
	role := "READER"
	if writable {
		role = "WRITER"
	}
	state, err := configuration.Load(ctx, configuration.Declaration[nacos.Settings]{
		Schema: configsource.Schema[nacos.Settings]{
			Version: 1,
			Defaults: nacos.Settings{
				Name: "configuration-service", DynamicKeys: true, Writable: writable,
				RequestTimeout: 5 * time.Second, ReconcileInterval: 5 * time.Minute,
				ConcurrentRequests: 8, Subscriptions: 3,
			},
			Validate: func(_ context.Context, value nacos.Settings) error { return nacos.Validate(value) },
		},
		Environment: []configuration.Environment{
			{Name: "FATHOMRY_LIVE_NACOS_SERVERS", Path: "/servers", JSON: true},
			{Name: "FATHOMRY_LIVE_NACOS_NAMESPACE", Path: "/namespace"},
			{Name: "FATHOMRY_LIVE_NACOS_" + role + "_USERNAME", Path: "/username"},
			{Name: "FATHOMRY_LIVE_NACOS_" + role + "_PASSWORD", Path: "/password"},
			{Name: "FATHOMRY_LIVE_NACOS_ALLOW_INSECURE", Path: "/allow_insecure", JSON: true},
		},
	}, deps)
	if err != nil {
		t.Fatal("Framework environment bootstrap failed")
	}
	accepted, err := state.Capture()
	if err != nil {
		t.Fatal("accepted bootstrap unavailable")
	}
	value, err := accepted.ValueCopy()
	if err != nil || value.Username == "" || value.Password == "" {
		t.Fatal("explicit bootstrap credentials unavailable")
	}
	return value
}

func configurationServiceNacos(t *testing.T, ctx context.Context, value nacos.Settings, deps nacos.Dependencies) *nacos.Client {
	t.Helper()
	owner, err := nacos.Open(ctx, value, deps)
	if owner != nil {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if owner.Close(ctx) != nil || !owner.ShutdownComplete() {
				t.Error("public Nacos owner shutdown incomplete")
			}
		})
	}
	if err != nil || owner == nil {
		t.Fatal("public Nacos owner creation failed")
	}
	return owner.Client()
}

func configurationServiceReadback(t *testing.T, ctx context.Context, client *nacos.Client, key nacos.Key, content string, missing bool) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		document, err := client.ReadRaw(wait, key)
		if err == nil && document.Missing() == missing && (missing || string(document.RawCopy()) == content) {
			return
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-wait.Done():
			t.Fatal("isolated Nacos content/presence was not observed")
		}
	}
}

func configurationServicePublish(t *testing.T, ctx context.Context, writer, reader *nacos.Client, key nacos.Key, content string) {
	t.Helper()
	result, err := writer.Publish(ctx, nacos.PublishInput{Key: key, Content: content, ContentType: "json"})
	if err != nil || result.State() != nacos.MutationAcknowledged {
		t.Fatal("isolated Nacos publication not acknowledged; mutation not retried")
	}
	configurationServiceReadback(t, ctx, reader, key, content, false)
}

func configurationServiceContent(t *testing.T, value configurationServiceSettings) string {
	t.Helper()
	database := value.Database
	raw, err := json.Marshal(map[string]any{
		"version": value.Version, "label": value.Label,
		"database": map[string]any{
			"address": database.Address, "port": database.Port, "user": database.User,
			"database": database.Database, "root_ca_pem": database.RootCAPEM,
			"server_name": database.ServerName, "plaintext": database.Plaintext,
			"pool_size": database.PoolSize, "reject_readiness": database.RejectReadiness,
		},
	})
	if err != nil {
		t.Fatal("isolated document serialization failed")
	}
	return string(raw)
}

func configurationServiceDecision(t *testing.T, ctx context.Context, watch *configuration.Watcher[configurationServiceSettings], version int, rejected error) configuration.Event {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		event, err := watch.Next(wait)
		if err != nil {
			t.Fatal("Framework Watch decision unavailable")
		}
		if rejected != nil {
			if !event.Accepted && !event.Superseded && errors.Is(event.Err, rejected) {
				return event
			}
			continue
		}
		if !event.Accepted {
			continue
		}
		current, err := watch.Capture()
		if err != nil {
			t.Fatal("Watch accepted data unavailable")
		}
		value, err := current.ValueCopy()
		if err == nil && value.Version == version && current.Sequence() == event.Sequence {
			if event.Status.AdoptionError != nil || event.Status.Adoption == nil {
				t.Fatal("accepted settings did not request resource adoption")
			}
			return event
		}
	}
}

func configurationServiceBinding(t *testing.T, scope *resource.Scope, name string, policy resource.Policy) resource.Ref[*serviceDatabase] {
	t.Helper()
	var serial atomic.Uint64
	ref, err := resource.Bind(scope, resource.Binding[configurationServiceDatabase, *serviceDatabase]{
		Name: name, Policy: policy,
		Select: func(view settings.View) (configurationServiceDatabase, error) {
			value, present, err := settings.Read(view, "/database",
				func(value configurationServiceDatabase) configurationServiceDatabase { return value })
			if err != nil {
				return configurationServiceDatabase{}, err
			}
			if !present {
				return configurationServiceDatabase{}, errConfigurationServiceSchema
			}
			return value, nil
		},
		Clone: func(value configurationServiceDatabase) configurationServiceDatabase { return value },
		Equal: func(left, right configurationServiceDatabase) bool { return left == right },
		Build: func(ctx context.Context, value configurationServiceDatabase) (*resource.Instance[*serviceDatabase], error) {
			// All connection values come from the selected accepted subsection, not an inventory closure.
			var fixture resourceServiceFixture
			fixture.Postgres.Address, fixture.Postgres.Port = value.Address, value.Port
			fixture.Postgres.User, fixture.Postgres.Password = value.User, value.Password
			fixture.Postgres.Database = value.Database
			fixture.Postgres.RootCAPEM, fixture.Postgres.ServerName = value.RootCAPEM, value.ServerName
			fixture.Postgres.Plaintext = value.Plaintext
			return buildServiceDatabase(ctx, fixture,
				serviceConfiguration{PoolSize: value.PoolSize, RejectReadiness: value.RejectReadiness},
				fmt.Sprintf("%s-%d", name, serial.Add(1)))
		},
	})
	if err != nil {
		t.Fatal("database resource binding failed")
	}
	return ref
}

func configurationServiceResult(t *testing.T, ctx context.Context, receipt *invocation.Receipt[pgx.Result], err error) pgx.Result {
	t.Helper()
	if err != nil || receipt == nil {
		t.Fatal("PostgreSQL operation admission failed")
	}
	result, err := receipt.WaitReleased(ctx)
	if err != nil || !result.Final || !result.Released || result.Err() != nil || !result.Outcome.Present {
		t.Fatal("PostgreSQL operation did not finish successfully")
	}
	return result.Outcome.Value
}

func configurationServiceCRUD(t *testing.T, ctx context.Context, database *serviceDatabase, table string) *pgx.Transaction {
	t.Helper()
	transaction, lifetime, err := database.database.Begin(ctx, fault.Correlation{Call: "temp-transaction"},
		pgx.TxOptionsV1{Isolation: sdk.ReadCommitted, Access: sdk.ReadWrite})
	if err != nil || transaction == nil {
		t.Fatal("PostgreSQL transaction creation failed")
	}
	// The caller finalizes successful transactions; this fallback also runs after Fatal.
	t.Cleanup(func() {
		if result, ok := lifetime.Result(); ok && result.Final && result.Outcome.Present &&
			(result.Outcome.Value.TransactionOutcome() == pgx.CommitAcknowledged || result.Outcome.Value.TransactionOutcome() == pgx.RollbackAcknowledged) {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		receipt, err := transaction.Rollback(ctx)
		if err != nil || receipt == nil {
			t.Error("temporary transaction rollback not admitted; pool cleanup still required")
			return
		}
		result, err := receipt.WaitReleased(ctx)
		if err != nil || result.Err() != nil || !result.Outcome.Present || result.Outcome.Value.TransactionOutcome() != pgx.RollbackAcknowledged {
			t.Error("temporary transaction rollback not confirmed; pool cleanup still required")
		}
	})
	exec := func(query string, args ...any) pgx.Result {
		t.Helper()
		receipt, err := transaction.Exec(ctx, fault.Correlation{Call: "temp-crud"}, query, args...)
		return configurationServiceResult(t, ctx, receipt, err)
	}
	query := func(query string) string {
		t.Helper()
		receipt, err := transaction.Query(ctx, fault.Correlation{Call: "temp-read"}, query)
		return serviceCell(t, configurationServiceResult(t, ctx, receipt, err))
	}
	exec("CREATE TEMP TABLE " + table + " (value bigint) ON COMMIT DROP")
	if exec("INSERT INTO "+table+" VALUES ($1)", int64(41)).RowsAffected() != 1 ||
		query("SELECT value FROM "+table) != "41" ||
		exec("UPDATE "+table+" SET value=$1", int64(42)).RowsAffected() != 1 ||
		query("SELECT value FROM "+table) != "42" ||
		exec("DELETE FROM "+table).RowsAffected() != 1 ||
		query("SELECT count(*) FROM "+table) != "0" ||
		exec("INSERT INTO "+table+" VALUES ($1)", int64(42)).RowsAffected() != 1 {
		t.Fatal("temporary-table CRUD did not preserve expected data/effects")
	}
	return transaction
}

func TestConfigurationServicePreparation(t *testing.T) {
	value := configurationServiceSettings{Version: 1, Label: "synthetic", Database: configurationServiceDatabase{
		Address: "127.0.0.1", Port: 5432, User: "synthetic", Password: "environment-only-canary",
		Database: "synthetic", Plaintext: true, PoolSize: 1,
	}}
	variables, err := configsource.BindVariables[configurationServiceSettings]([]configsource.Variable{
		{Path: "/database/password", Value: value.Database.Password, Present: true},
	})
	if err != nil {
		t.Fatal("test variable declaration rejected")
	}
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid=%t", invalid), func(t *testing.T) {
			input := value
			if invalid {
				input.Database.PoolSize = 0
			}
			raw := configurationServiceContent(t, input)
			var document map[string]json.RawMessage
			if json.Unmarshal([]byte(raw), &document) != nil {
				t.Fatal("isolated document is not JSON")
			}
			var database map[string]json.RawMessage
			if json.Unmarshal(document["database"], &database) != nil || database["password"] != nil {
				t.Fatal("remote document must not contain the environment password field")
			}
			prepared, err := configsource.Prepare(context.Background(), configurationServiceSchema(), []configsource.Layer{
				{Kind: configsource.Base, Encoding: configsource.JSON, Content: []byte(raw)}, variables,
			})
			if invalid {
				if !errors.Is(err, errConfigurationServiceSchema) {
					t.Fatal("invalid pool size was not rejected by the application schema")
				}
				return
			}
			if err != nil {
				t.Fatal("test schema/remote/environment preparation failed")
			}
			actual, err := prepared.ValueCopy()
			if err != nil || actual != value {
				t.Fatal("remote fields plus explicit environment password did not round-trip")
			}
		})
	}
}

func TestConfigurationService(t *testing.T) {
	if os.Getenv("FATHOMRY_LIVE_ALLOW_WRITES") != "1" {
		t.Skip("requires explicit environment bootstrap and isolated-write authorization")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	runtime, nativeDeps, configDeps := configurationServiceRuntime(t, ctx)
	writerSettings := configurationServiceBootstrap(t, ctx, configDeps, true)
	readerSettings := configurationServiceBootstrap(t, ctx, configDeps, false)
	writer := configurationServiceNacos(t, ctx, writerSettings, nativeDeps)
	reader := configurationServiceNacos(t, ctx, readerSettings, nativeDeps)

	var nonce [12]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("isolated identity generation failed")
	}
	identity := hex.EncodeToString(nonce[:])
	key := nacos.Key{Group: "DEFAULT_GROUP", DataID: "gh98-configuration-" + identity + ".json"}
	configurationServiceReadback(t, ctx, writer, key, "", true)
	configurationServiceReadback(t, ctx, reader, key, "", true)
	t.Logf("Owned isolated Nacos fixture: %s", key.DataID)
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 35*time.Second)
		defer stop()
		_, cleanupDeps, _ := configurationServiceRuntime(t, cleanup)
		client := configurationServiceNacos(t, cleanup, writerSettings, cleanupDeps)
		result, err := client.Delete(cleanup, key)
		if err != nil || result.State() != nacos.MutationAcknowledged {
			t.Errorf("fixture deletion not acknowledged: %s; mutation not retried", key.DataID)
		}
		configurationServiceReadback(t, cleanup, client, key, "", true)
		t.Log("PASS: isolated Nacos deletion and positive absence confirmed with a fresh owner")
	})
	t.Log("PASS: environment -> Framework Load -> typed Nacos Settings -> authenticated reader/writer")

	seedSchema := configurationServiceSchema()
	seedSchema.Defaults = configurationServiceSettings{Version: 1, Label: "initial", Database: configurationServiceDatabase{PoolSize: 1}}
	seed, err := configuration.Load(ctx, configuration.Declaration[configurationServiceSettings]{
		Schema: seedSchema,
		Environment: []configuration.Environment{
			{Name: "FATHOMRY_LIVE_DB_ADDRESS", Path: "/database/address"},
			{Name: "FATHOMRY_LIVE_DB_PORT", Path: "/database/port", JSON: true},
			{Name: "FATHOMRY_LIVE_DB_USER", Path: "/database/user"},
			{Name: "FATHOMRY_LIVE_DB_PASSWORD", Path: "/database/password"},
			{Name: "FATHOMRY_LIVE_DB_DATABASE", Path: "/database/database"},
			{Name: "FATHOMRY_LIVE_DB_PLAINTEXT", Path: "/database/plaintext", JSON: true},
		},
	}, configDeps)
	if err != nil {
		t.Fatal("isolated application seed environment loading failed")
	}
	seedAccepted, err := seed.Capture()
	if err != nil {
		t.Fatal("isolated seed unavailable")
	}
	expected, err := seedAccepted.ValueCopy()
	if err != nil {
		t.Fatal("isolated seed copy failed")
	}
	content := configurationServiceContent(t, expected)
	configurationServicePublish(t, ctx, writer, reader, key, content)
	source, err := reader.Source(nacos.ObserveOptions{QueueCapacity: 4}, key)
	if err != nil {
		t.Fatal("public Nacos Source selection failed")
	}
	declaration := configuration.Declaration[configurationServiceSettings]{
		Schema: configurationServiceSchema(), Source: source,
		Layers:      []configuration.Layer{{Kind: configsource.Base, Encoding: configsource.JSON}},
		Environment: []configuration.Environment{{Name: "FATHOMRY_LIVE_DB_PASSWORD", Path: "/database/password"}},
	}
	loaded, err := configuration.Load(ctx, declaration, configDeps)
	if err != nil {
		t.Fatal("real Nacos -> Framework Load failed")
	}
	loadedView, err := loaded.Reader().Capture()
	if err != nil {
		t.Fatal("loaded settings reader unavailable")
	}
	selected, present, err := settings.Read(loadedView, "/database",
		func(value configurationServiceDatabase) configurationServiceDatabase { return value })
	if err != nil || !present || selected != expected.Database {
		t.Fatal("finite loader did not preserve remote database settings plus environment secret")
	}
	t.Log("PASS: Nacos -> Framework Load -> independent settings reader -> database subsection")

	follow := configurationServiceBinding(t, runtime.Resources(), "following", resource.Follow)
	fixed := configurationServiceBinding(t, runtime.Resources(), "fixed", resource.Fixed)
	watchDeps := configDeps
	watchDeps.Resources = runtime.Resources()
	watch, err := configuration.Watch(ctx, declaration, watchDeps, configuration.WatchOptions{QueueCapacity: 32})
	if err != nil {
		t.Fatal("Framework Watch startup failed")
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if watch.Close(cleanup) != nil {
			t.Error("Framework Watch cleanup incomplete")
		}
	})
	initial := configurationServiceDecision(t, ctx, watch, expected.Version, nil)
	if initial.Status.Adoption.Wait(ctx) != nil {
		t.Fatal("initial database resources not ready")
	}
	fixedBefore, err := fixed.Inspect()
	if err != nil || !fixedBefore.Active {
		t.Fatal("fixed database resource unavailable")
	}
	old, err := follow.Acquire(ctx)
	if err != nil {
		t.Fatal("initial following database borrow failed")
	}
	t.Cleanup(func() { _ = old.Release() })
	before, err := old.Value()
	if err != nil || before.database.Stats().MaxResources() != int32(expected.Database.PoolSize) {
		t.Fatal("selected database pool size not applied")
	}
	table := "configuration_" + identity
	transaction := configurationServiceCRUD(t, ctx, before, table)
	oldQuery := func(sql string, args ...any) pgx.Result {
		t.Helper()
		receipt, err := transaction.Query(ctx, fault.Correlation{Call: "retained-transaction"}, sql, args...)
		return configurationServiceResult(t, ctx, receipt, err)
	}
	oldPID := serviceCell(t, oldQuery("SELECT pg_backend_pid()"))
	t.Log("PASS: Watch -> accepted settings -> resource Build -> real PostgreSQL temporary-table CRUD")

	expected.Version++
	expected.Label = "unrelated-change"
	configurationServicePublish(t, ctx, writer, reader, key, configurationServiceContent(t, expected))
	unrelated := configurationServiceDecision(t, ctx, watch, expected.Version, nil)
	if unrelated.Status.Adoption.Wait(ctx) != nil {
		t.Fatal("unrelated settings adoption failed")
	}
	unchanged, err := follow.Inspect()
	if err != nil || unchanged.Generation != old.Generation() {
		t.Fatal("unrelated settings rebuilt the selected database subsection")
	}
	lastGood, err := watch.Capture()
	if err != nil {
		t.Fatal("last-good settings unavailable")
	}
	reject := func(content string, problem error) {
		t.Helper()
		configurationServicePublish(t, ctx, writer, reader, key, content)
		configurationServiceDecision(t, ctx, watch, 0, problem)
		current, err := watch.Capture()
		if err != nil || current.Sequence() != lastGood.Sequence() {
			t.Fatal("rejected source document replaced last-good settings")
		}
		status, err := follow.Inspect()
		if err != nil || !status.Active || status.Generation != old.Generation() {
			t.Fatal("rejected source document replaced the active resource")
		}
	}
	reject("{", configsource.ErrDecode)
	invalid := expected
	invalid.Database.PoolSize = 0
	reject(configurationServiceContent(t, invalid), errConfigurationServiceSchema)
	deleted, err := writer.Delete(ctx, key)
	if err != nil || deleted.State() != nacos.MutationAcknowledged {
		t.Fatal("isolated missing-source mutation not acknowledged; mutation not retried")
	}
	configurationServiceReadback(t, ctx, reader, key, "", true)
	configurationServiceDecision(t, ctx, watch, 0, configuration.ErrMissing)
	retained, err := watch.Capture()
	if err != nil || retained.Sequence() != lastGood.Sequence() {
		t.Fatal("missing required source replaced last-good settings")
	}
	t.Log("PASS: unrelated settings reuse; malformed, schema-invalid and deleted source retain last-good")

	expected.Version++
	expected.Database.PoolSize = 3
	expected.Database.RejectReadiness = true
	configurationServicePublish(t, ctx, writer, reader, key, configurationServiceContent(t, expected))
	failed := configurationServiceDecision(t, ctx, watch, expected.Version, nil)
	var nativeFailure *pgconn.PgError
	if err := failed.Status.Adoption.Wait(ctx); !errors.Is(err, resource.ErrBuild) ||
		!errors.As(err, &nativeFailure) || nativeFailure.Code != "22012" {
		t.Fatal("resource construction failure lost native readiness SQLSTATE or public classification")
	}
	failedStatus, err := follow.Inspect()
	if err != nil || !failedStatus.Active || failedStatus.Generation != old.Generation() ||
		serviceCell(t, oldQuery("SELECT value FROM "+table)) != "42" {
		t.Fatal("failed replacement lost the usable old database generation")
	}
	t.Log("PASS: settings acceptance is separate from database readiness; SQLSTATE 22012 preserves old pool")

	expected.Version++
	expected.Database.PoolSize = 4
	expected.Database.RejectReadiness = false
	configurationServicePublish(t, ctx, writer, reader, key, configurationServiceContent(t, expected))
	recovered := configurationServiceDecision(t, ctx, watch, expected.Version, nil)
	if recovered.Status.Adoption.Wait(ctx) != nil {
		t.Fatal("valid replacement did not recover after readiness failure")
	}
	current, err := follow.Acquire(ctx)
	if err != nil {
		t.Fatal("replacement database borrow failed")
	}
	t.Cleanup(func() { _ = current.Release() })
	after, err := current.Value()
	if err != nil || current.Generation() == old.Generation() || after.database.Stats().MaxResources() != 4 {
		t.Fatal("Follow did not adopt the new pool configuration")
	}
	receipt, err := after.database.Query(ctx, fault.Correlation{Call: "replacement-session"},
		"SELECT to_regclass($1) IS NULL AND pg_backend_pid()::text<>$2::text", "pg_temp."+table, oldPID)
	if serviceCell(t, configurationServiceResult(t, ctx, receipt, err)) != "t" {
		t.Fatal("new pool reused old transaction/session state")
	}
	replacementTx := configurationServiceCRUD(t, ctx, after, table+"_new")
	receipt, err = replacementTx.Query(ctx, fault.Correlation{Call: "replacement-pid"}, "SELECT pg_backend_pid()")
	replacementPID := serviceCell(t, configurationServiceResult(t, ctx, receipt, err))
	receipt, err = replacementTx.Commit(ctx)
	if configurationServiceResult(t, ctx, receipt, err).TransactionOutcome() != pgx.CommitAcknowledged {
		t.Fatal("replacement temporary transaction commit not acknowledged")
	}
	receipt, err = after.database.Query(ctx, fault.Correlation{Call: "replacement-temp-removed"},
		"SELECT to_regclass($1) IS NULL AND pg_backend_pid()::text=$2::text", "pg_temp."+table+"_new", replacementPID)
	if serviceCell(t, configurationServiceResult(t, ctx, receipt, err)) != "t" {
		t.Fatal("replacement temporary table removal in its original session not confirmed")
	}
	fixedAfter, err := fixed.Inspect()
	if err != nil || fixedAfter.Generation != fixedBefore.Generation {
		t.Fatal("Fixed followed a changed database configuration")
	}
	if serviceCell(t, oldQuery("SELECT value FROM "+table)) != "42" ||
		serviceCell(t, oldQuery("SELECT pg_backend_pid()")) != oldPID {
		t.Fatal("old borrowed transaction lost data or changed sessions")
	}
	if current.Release() != nil {
		t.Fatal("replacement lease release failed")
	}
	t.Log("PASS: Follow replaces pool 1 -> 4, Fixed stays; old transaction/session and new-pool CRUD remain usable")

	if watch.Close(ctx) != nil {
		t.Fatal("Watch did not stop before resource shutdown")
	}
	wait, stop := context.WithTimeout(ctx, 25*time.Millisecond)
	err = runtime.Resources().Close(wait)
	stop()
	if !errors.Is(err, resource.ErrWait) ||
		serviceCell(t, oldQuery("SELECT value FROM "+table)) != "42" {
		t.Fatal("bounded shutdown abandoned the borrowed transaction")
	}
	receipt, err = transaction.Commit(ctx)
	if configurationServiceResult(t, ctx, receipt, err).TransactionOutcome() != pgx.CommitAcknowledged {
		t.Fatal("retained temporary transaction commit not acknowledged")
	}
	receipt, err = before.database.Query(ctx, fault.Correlation{Call: "temp-removed"},
		"SELECT to_regclass($1) IS NULL AND pg_backend_pid()::text=$2::text", "pg_temp."+table, oldPID)
	if serviceCell(t, configurationServiceResult(t, ctx, receipt, err)) != "t" {
		t.Fatal("temporary table removal in the original session not confirmed")
	}
	if old.Release() != nil || runtime.Resources().Close(ctx) != nil {
		t.Fatal("retained database generation cleanup incomplete")
	}
	if initial.Status.Adoption.Wait(ctx) != nil {
		t.Fatal("shutdown rewrote historical adoption evidence")
	}
	t.Log("PASS: bounded Close retains borrow; commit removes temporary table; leases/pools shut down")
}
