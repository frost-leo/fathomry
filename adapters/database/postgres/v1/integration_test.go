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

package postgres

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"debug/buildinfo"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"log/slog"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/framework/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
	sdk "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
)

func TestSharedSourceNativeCapacity(t *testing.T) {
	peer := newProtocolPeer(t, false)
	selected := peer.options()
	selected.MaxConnections = 1
	owner, inbox, _ := testOwner(t, selected, 0)
	policy, err := Recommend(selected)
	if err != nil {
		t.Fatal(err)
	}
	independent, err := adapters.New(context.Background(), policy.Runtime)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := independent.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	client := testUsingClient(t, context.Background(), owner, inbox, independent)
	transaction, err := owner.Client().Begin(context.Background(), txOptions())
	if err != nil {
		t.Fatal(err)
	}
	before := peer.queries.Load()
	rejected, err := client.Query(context.Background(), "SELECT cells")
	core, recognized := failure.Inspect(err)
	if !recognized || core.Diagnostic().Definition.Code != ErrLimit || !errors.Is(err, ErrLimit) {
		t.Fatal("native source capacity lost its public limit identity", err)
	}
	if peer.queries.Load() != before || rejected.HasData() || rejected.Complete() {
		t.Fatal("native capacity rejection dispatched SQL or invented data")
	}
	delivery := receive(t, inbox)
	claimed := true
	defer func() {
		if claimed {
			if err := delivery.Ack(); err != nil {
				t.Error("rejected operation evidence cleanup failed", err)
			}
		}
	}()
	receipt, err := delivery.Receipt()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, resolved := receipt.Snapshot()
	if !resolved || !snapshot.Info().Released || !errors.Is(snapshot.Primary(), ErrLimit) ||
		snapshot.Cleanup() != nil || snapshot.Info().Source.Name != "database" || snapshot.Info().Source.Generation != 1 {
		t.Fatal("capacity rejection lost independent evidence or source attribution")
	}
	if err := delivery.Ack(); err != nil {
		t.Fatal(err)
	}
	claimed = false
	if value, err := transaction.Rollback(context.Background()); err != nil || value.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("original transaction could not finalize", err)
	}
	ack(t, inbox)
	if value, err := client.Query(context.Background(), "SELECT cells"); err != nil || !value.Complete() {
		t.Fatal("native capacity was not reusable after finalization", err)
	}
	ack(t, inbox)
	if state, err := independent.Inspect(); err != nil || state.Active != 0 || state.WorkBytes != 0 {
		t.Fatal("shared-source rejection leaked public admission", err)
	}
	if state, err := inbox.Inspect(); err != nil || state.Outstanding != 1 {
		t.Fatal("shared-source rejection leaked required evidence", err)
	}
}

func TestPublicSlice(t *testing.T) {
	peer := newProtocolPeer(t, true)
	owner, inbox, _ := testOwner(t, peer.options(), 0)
	client, err := owner.Client().WithID("测试/correlation")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if value, err := client.Ping(ctx); err != nil || !value.Complete() {
		t.Fatal("ping", err)
	}
	ack(t, inbox)
	value, err := client.Query(ctx, "SELECT cells")
	if err != nil || !value.Complete() || len(value.ColumnsCopy()) != 3 || value.Attribution().ID != "测试/correlation" {
		t.Fatal("query", err)
	}
	rows := value.RowsCopy()
	cells := rows[0].ValuesCopy()
	if cells[0] != nil || cells[1] == nil || len(cells[1]) != 0 || len(cells[2]) == 0 {
		t.Fatal("NULL/empty contract")
	}
	cells[2][0] = 'X'
	if string(rows[0].ValuesCopy()[2]) == string(cells[2]) {
		t.Fatal("cell alias")
	}
	columns := value.ColumnsCopy()
	columns[0].Name = "changed"
	if value.ColumnsCopy()[0].Name == "changed" {
		t.Fatal("column alias")
	}
	if value.Source().Name != "fixture" || value.Source().Revision == "" || value.Attempts().Observed == 0 {
		t.Fatal("attribution missing")
	}
	ack(t, inbox)
	empty, err := client.Query(ctx, "SELECT empty")
	if err != nil || empty.RowsCopy() == nil || len(empty.RowsCopy()) != 0 {
		t.Fatal("empty query", err)
	}
	if _, err = empty.First(); !errors.Is(err, sdk.ErrNoRows) {
		t.Fatal("no-row cause", err)
	}
	ack(t, inbox)
	partial, err := client.Query(ctx, "SELECT partial")
	if err == nil || partial.Complete() || len(partial.RowsCopy()) != 1 {
		t.Fatal("partial query", err)
	}
	if _, ok := InspectError(err); !ok {
		t.Fatal("native server evidence lost")
	}
	ack(t, inbox)
	setup, cancel := context.WithCancel(ctx)
	statement, err := client.Prepare(setup, "SELECT $1::text")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if !statement.Ready().Complete() {
		t.Fatal("preparation not ready")
	}
	if snapshot, _ := statement.Receipt().Snapshot(); snapshot.Info().Resolved {
		t.Fatal("initial readiness became final evidence")
	}
	for range 3 {
		result, err := statement.Query(ctx, "exact")
		if err != nil || !result.Complete() {
			t.Fatal("retained statement", err)
		}
		ack(t, inbox)
	}
	if _, err := statement.Close(ctx); err != nil {
		t.Fatal(err)
	}
	ack(t, inbox)
	transaction, err := client.Begin(ctx, txOptions())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := transaction.Prepare(ctx, "SELECT $1::text")
	if err != nil {
		t.Fatal(err)
	}
	if value, err := prepared.Query(ctx, "inside"); err != nil || !value.Complete() {
		t.Fatal("prepared tx", err)
	}
	ack(t, inbox)
	committed, err := transaction.Commit(ctx)
	if err != nil || committed.TransactionOutcome() != CommitAcknowledged {
		t.Fatal("commit", err)
	}
	if snapshot, _ := prepared.Receipt().Snapshot(); !snapshot.Info().Released {
		t.Fatal("child preparation not joined")
	}
	ack(t, inbox)
	ack(t, inbox)
	transaction, err = client.Begin(ctx, txOptions())
	if err != nil {
		t.Fatal(err)
	}
	rolled, err := transaction.Rollback(ctx)
	if err != nil || rolled.TransactionOutcome() != RollbackAcknowledged {
		t.Fatal("rollback", err)
	}
	ack(t, inbox)
}

func TestResourceFrameworkComposition(t *testing.T) {
	firstPeer, secondPeer := newProtocolPeer(t, true), newProtocolPeer(t, true)
	first, second := firstPeer.options(), secondPeer.options()
	first.Name, second.Name = "first", "second"
	first.MaxConnections, second.MaxConnections = 2, 2
	policy, err := Recommend(first)
	if err != nil {
		t.Fatal(err)
	}
	policy.Runtime.MaxActive *= 4
	policy.Runtime.MaxWorkBytes *= 4
	policy.Evidence.Capacity *= 4
	policy.Evidence.MaxBytes *= 4
	runtime, err := framework.New(context.Background(), framework.Options{Operations: policy.Runtime})
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := adapters.NewInbox[Result](policy.Evidence)
	if err != nil {
		t.Fatal(err)
	}
	dependencies := Dependencies{Runtime: runtime.Operations(), Evidence: inbox}
	var owners []*Owner
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(cleanup); err != nil {
			t.Error("Framework shutdown", err)
		}
		for _, owner := range owners {
			if !owner.ShutdownComplete() {
				t.Error("Framework abandoned source ownership")
			}
		}
		for status, _ := inbox.Inspect(); status.Outstanding > 0; status, _ = inbox.Inspect() {
			delivery, err := inbox.NextReleased(cleanup)
			if err != nil {
				t.Error(err)
				break
			}
			if err = delivery.Ack(); err != nil {
				t.Error(err)
				break
			}
		}
	})
	type construction struct {
		binding string
		owner   *Owner
	}
	opened := make(chan construction, 8)
	released := make(chan *Owner, 8)
	refusal := errors.New("candidate-refused")
	bind := func(name string, policy resource.Policy) resource.Ref[Handle] {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[Settings, Handle]{
			Name: name, Policy: policy,
			Select: func(view settings.View) (Settings, error) {
				snapshot, err := settings.As[Settings](view)
				if err != nil {
					return Settings{}, err
				}
				return snapshot.ValueCopy()
			},
			Clone: func(value Settings) Settings { return value },
			Equal: func(left, right Settings) bool { return left == right },
			Build: func(ctx context.Context, value Settings) (*resource.Instance[Handle], error) {
				owner, err := Open(ctx, value, dependencies)
				if owner == nil {
					return nil, err
				}
				opened <- construction{name, owner}
				var notified sync.Once
				instance := &resource.Instance[Handle]{Value: owner.Handle(), Release: func(ctx context.Context) resource.ReleaseResult {
					result := owner.Release(ctx)
					if result.Complete {
						notified.Do(func() { released <- owner })
					}
					return result
				}}
				if value.Name == "refused" {
					return instance, errors.Join(err, refusal)
				}
				return instance, err
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixedRef, followRef := bind("fixed", resource.Fixed), bind("follow", resource.Follow)
	apply := func(value Settings) error {
		snapshot, err := settings.New(value, func(value Settings) Settings { return value })
		if err != nil {
			t.Fatal(err)
		}
		setup, cancel := context.WithCancel(context.Background())
		update, err := runtime.Resources().Apply(setup, snapshot.View())
		if err != nil {
			cancel()
			return err
		}
		wait, stop := context.WithTimeout(context.Background(), 3*time.Second)
		err = update.Wait(wait)
		stop()
		cancel()
		return err
	}
	constructed := func() construction {
		select {
		case value := <-opened:
			owners = append(owners, value.owner)
			return value
		case <-time.After(3 * time.Second):
			t.Fatal("source construction did not return")
			return construction{}
		}
	}
	finalized := func() *Owner {
		select {
		case owner := <-released:
			return owner
		case <-time.After(3 * time.Second):
			t.Fatal("retired source cleanup did not finish")
			return nil
		}
	}
	if err := apply(first); err != nil {
		t.Fatal(err)
	}
	var old *Owner
	for range 2 {
		value := constructed()
		if value.binding == "follow" {
			old = value.owner
		}
	}
	fixed, err := Using(context.Background(), fixedRef, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	follow, err := Using(context.Background(), followRef, policy.Budget, dependencies)
	if err != nil {
		t.Fatal(err)
	}
	fixedStatus, _ := fixedRef.Inspect()
	oldStatus, _ := followRef.Inspect()
	if value, err := follow.Ping(context.Background()); err != nil || !value.Complete() {
		t.Fatal("explicit readiness", err)
	}
	transaction, err := follow.Begin(context.Background(), txOptions())
	if err != nil {
		t.Fatal(err)
	}
	statement, err := follow.Prepare(context.Background(), "SELECT $1::text")
	if err != nil {
		t.Fatal(err)
	}
	if status, _ := followRef.Inspect(); status.Borrowers != 2 {
		t.Fatal("retained roots did not each pin their generation")
	}
	if err := apply(second); err != nil {
		t.Fatal(err)
	}
	current := constructed()
	status, _ := followRef.Inspect()
	if current.binding != "follow" || status.Generation == oldStatus.Generation || status.Retiring != 1 || old.ShutdownComplete() {
		t.Fatal("replacement revoked retained generation")
	}
	if status, _ := fixedRef.Inspect(); status.Generation != fixedStatus.Generation {
		t.Fatal("Fixed source followed replacement")
	}
	query := func(client *Client, sourceName string, generation uint64) Result {
		value, err := client.Query(context.Background(), "SELECT cells")
		if err != nil || !value.Complete() || value.Source().Name != sourceName || value.Attribution().Source.Generation != generation {
			t.Fatal("source/generation attribution changed", err)
		}
		return value
	}
	beforeFirst, beforeSecond := firstPeer.queries.Load(), secondPeer.queries.Load()
	query(follow, "second", status.Generation)
	if firstPeer.queries.Load() != beforeFirst || secondPeer.queries.Load() <= beforeSecond {
		t.Fatal("Follow did not use adopted pool")
	}
	query(fixed, "first", fixedStatus.Generation)
	beforeSecond = secondPeer.queries.Load()
	value, err := transaction.Query(context.Background(), "SELECT cells")
	if err != nil || value.Source().Name != "first" || value.Attribution().Source.Generation != oldStatus.Generation {
		t.Fatal("transaction retargeted", err)
	}
	value, err = statement.Query(context.Background(), "retained")
	if err != nil || value.Source().Name != "first" || value.Attribution().Source.Generation != oldStatus.Generation || secondPeer.queries.Load() != beforeSecond {
		t.Fatal("preparation retargeted", err)
	}
	if _, err := transaction.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if old.ShutdownComplete() {
		t.Fatal("one family released another family's lease")
	}
	if _, err := statement.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if finalized() != old || !old.ShutdownComplete() {
		t.Fatal("wrong retired source released")
	}
	rejected := second
	rejected.Name = "refused"
	if err := apply(rejected); !errors.Is(err, refusal) {
		t.Fatal("partial replacement lost original cause", err)
	}
	candidate := constructed()
	if finalized() != candidate.owner || !candidate.owner.ShutdownComplete() {
		t.Fatal("failed candidate was not retained through cleanup")
	}
	if after, _ := followRef.Inspect(); after.Generation != status.Generation {
		t.Fatal("failed replacement discarded last good instance")
	}
	query(follow, "second", status.Generation)
	_, operationErr := follow.Query(context.Background(), "SELECT partial")
	if operationErr == nil {
		t.Fatal("accepted query failure hidden")
	}
	if _, ok := failure.Inspect(operationErr); !ok {
		t.Fatal("direct error hid public semantic occurrence")
	}
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "database_postgres", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := i18n.NewPresenter(catalog)
	if err != nil {
		t.Fatal(err)
	}
	presenter, err = presenter.WithLocale("zh-CN")
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	emission := framework.NewErrorLog(slog.New(slog.NewJSONHandler(&output, nil)), presenter).Emit(context.Background(), operationErr)
	if !emission.Recognized || emission.Issue != nil || !errors.Is(emission.Presented, operationErr) || strings.Contains(output.String(), "canary") {
		t.Fatal("Framework presentation erased semantic evidence or exposed native text")
	}
	originalServer, originalFound := InspectError(operationErr)
	presentedServer, presentedFound := InspectError(emission.Presented)
	if !originalFound || !presentedFound || originalServer != presentedServer {
		t.Fatal("Framework presentation replaced native cause")
	}
	larger := second
	larger.Name = "larger"
	larger.MaxResultBytes *= 2
	if err := apply(larger); err != nil {
		t.Fatal(err)
	}
	constructed()
	beforeSecond = secondPeer.queries.Load()
	if _, err := follow.Query(context.Background(), "SELECT cells"); !errors.Is(err, ErrLimit) || secondPeer.queries.Load() != beforeSecond {
		t.Fatal("larger replacement dispatched undercharged work", err)
	}
}

func TestIndependentConsumer(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	fixture, err := os.ReadFile("testdata/consumer/main.go")
	if err != nil {
		t.Fatal(err)
	}
	notice, _, _ := strings.Cut(string(fixture), "package main")
	syntax, err := parser.ParseFile(token.NewFileSet(), "main.go", fixture, parser.ImportsOnly)
	if err != nil {
		t.Fatal(err)
	}
	for _, imported := range syntax.Imports {
		path, err := strconv.Unquote(imported.Path.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(path, "/internal/") || strings.Contains(path, "/framework/") {
			t.Fatal("consumer bypassed public Adapter boundary")
		}
	}
	sums, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string][]byte{
		"main.go": fixture,
		"go.sum":  sums,
		"go.mod":  []byte(fmt.Sprintf("module example.org/postgres-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
	} {
		if err := os.WriteFile(filepath.Join(directory, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	run := func(args ...string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", args...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		output, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent build exceeded bound")
		}
		return output, err
	}
	if output, err := run("mod", "tidy"); err != nil {
		t.Fatalf("consumer module preparation: %v\n%s", err, output)
	}
	binary := filepath.Join(directory, "consumer")
	if output, err := run("build", "-mod=readonly", "-race", "-o", binary, "."); err != nil {
		t.Fatalf("consumer build: %v\n%s", err, output)
	}
	if output, err := run("mod", "tidy", "-diff"); err != nil {
		t.Fatalf("consumer graph unstable: %v\n%s", err, output)
	}
	output, err := run("list", "-mod=readonly", "-deps", ".")
	if err != nil {
		t.Fatalf("consumer dependency inspection: %v\n%s", err, output)
	}
	for _, path := range strings.Fields(string(output)) {
		if strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/mysql/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/database/mysql/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/framework/") {
			t.Fatalf("single-provider consumer acquired unrelated dependency: %s", path)
		}
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, module := range info.Deps {
		if module.Path == "github.com/jackc/pgx/v5" {
			found = module.Version == "v5.11.0" && module.Replace == nil && module.Sum != ""
		}
	}
	if !found {
		t.Fatal("consumer binary does not use the selected native SDK")
	}
	peer := newProtocolPeer(t, true)
	input, err := json.Marshal(peer.options())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Stdin = bytes.NewReader(input)
	if output, err := command.CombinedOutput(); err != nil || string(output) != "postgres public consumer passed\n" {
		t.Fatalf("independent protocol consumer: %v\n%s", err, output)
	}
	for _, check := range []struct{ name, source, want string }{
		{"client_shutdown", "var _ = (*p.Client).Close", "has no field or method Close"},
		{"raw_connection", "var _ = (*p.Client).Conn", "has no field or method Conn"},
		{"handle_ownership", "var _ = p.Owner(p.Handle{})", "cannot convert"},
	} {
		t.Run(check.name, func(t *testing.T) {
			content := notice + "package main\nimport p \"github.com/frost-leo/fathomry/adapters/database/postgres/v1\"\n" + check.source + "\n"
			if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if output, err := run("build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), check.want) {
				t.Fatalf("authority boundary not enforced: %v\n%s", err, output)
			}
		})
	}
	private := "github.com/frost-leo/fathomry/internal/database/pgx/v5"
	if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(notice+"package main\nimport _ "+strconv.Quote(private)+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if output, err := run("build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), "use of internal package "+private+" not allowed") {
		t.Fatalf("Internal boundary not enforced: %v\n%s", err, output)
	}
	t.Log("Independent executable exercised the public protocol path; this is not real-service qualification.")
}

func testUsingClient(t testing.TB, lifetime context.Context, owner *Owner, inbox *adapters.Inbox[Result], runtime *adapters.Runtime) *Client {
	t.Helper()
	scope, err := resource.New(context.Background(), resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := scope.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	ref, err := resource.Bind(scope, resource.Binding[bool, Handle]{
		Name: "database", Policy: resource.Fixed,
		Select: func(settings.View) (bool, error) { return true, nil },
		Clone:  func(value bool) bool { return value },
		Build: func(context.Context, bool) (*resource.Instance[Handle], error) {
			return &resource.Instance[Handle]{Value: owner.Handle()}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.New(true, func(value bool) bool { return value })
	if err != nil {
		t.Fatal(err)
	}
	update, err := scope.Apply(context.Background(), snapshot.View())
	if err != nil {
		t.Fatal(err)
	}
	if err := update.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	client, err := Using(lifetime, ref, owner.state.policy.Budget, Dependencies{Runtime: runtime, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestUsingRetainedLifetime(t *testing.T) {
	for _, shape := range []string{"statement", "transaction"} {
		t.Run(shape, func(t *testing.T) {
			peer := newProtocolPeer(t, false)
			owner, inbox, runtime := testOwner(t, peer.options(), 0)
			lifetime, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := testUsingClient(t, lifetime, owner, inbox, runtime)
			var receipt *adapters.Receipt[Result]
			if shape == "statement" {
				setup, stopSetup := context.WithCancel(context.Background())
				statement, err := client.Prepare(setup, "SELECT cells")
				if err != nil {
					t.Fatal(err)
				}
				stopSetup()
				if _, err := statement.Query(context.Background()); err != nil {
					t.Fatal("setup ended retained preparation", err)
				}
				receipt = statement.Receipt()
			} else {
				transaction, err := client.Begin(context.Background(), txOptions())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := transaction.Query(context.Background(), "SELECT cells"); err != nil {
					t.Fatal("return ended retained transaction", err)
				}
				receipt = transaction.Receipt()
			}
			ack(t, inbox)
			cancel()
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			snapshot, err := receipt.WaitReleased(ctx)
			if err != nil {
				t.Fatal("client lifetime did not finalize retained work", err)
			}
			if shape == "transaction" {
				value, present := snapshot.ValueCopy()
				if !present || value.TransactionOutcome() != RollbackAcknowledged {
					t.Fatal("missing automatic rollback evidence")
				}
			}
			ack(t, inbox)
			if _, err := owner.Client().Ping(context.Background()); err != nil {
				t.Fatal("non-owning client closed source", err)
			}
			ack(t, inbox)
		})
	}
}

// protocolPeer executes client protocol paths only. It is NOT PostgreSQL and
// cannot establish database atomicity, server authorization or durability.
type protocolPeer struct {
	listener       net.Listener
	tls            *tls.Config
	roots          string
	mu             sync.Mutex
	sockets        map[net.Conn]struct{}
	cancel         map[uint32]context.CancelFunc
	workers        sync.WaitGroup
	stop           chan struct{}
	entered        chan struct{}
	queries        atomic.Int64
	connects       atomic.Int64
	commits        atomic.Int64
	rollbacks      atomic.Int64
	dropCommit     atomic.Bool
	blockStartup   atomic.Bool
	failReset      atomic.Bool
	failDeallocate atomic.Bool
	dropRollback   atomic.Bool
}

func newProtocolPeer(t testing.TB, secure bool) *protocolPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("loopback peer could not listen")
	}
	peer := &protocolPeer{listener: listener, sockets: make(map[net.Conn]struct{}), cancel: make(map[uint32]context.CancelFunc), stop: make(chan struct{}), entered: make(chan struct{}, 32)}
	if secure {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"fixture.invalid"}, NotBefore: time.Now().Add(-time.Hour),
			NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
		if err != nil {
			t.Fatal(err)
		}
		peer.roots = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		peer.tls = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}, MinVersion: tls.VersionTLS12}
	}
	peer.workers.Go(func() {
		for {
			socket, err := listener.Accept()
			if err != nil {
				return
			}
			peer.mu.Lock()
			select {
			case <-peer.stop:
				peer.mu.Unlock()
				socket.Close()
				return
			default:
			}
			peer.sockets[socket] = struct{}{}
			peer.mu.Unlock()
			peer.workers.Go(func() {
				defer socket.Close()
				defer func() { peer.mu.Lock(); delete(peer.sockets, socket); peer.mu.Unlock() }()
				peer.serve(socket)
			})
		}
	})
	t.Cleanup(func() {
		close(peer.stop)
		listener.Close()
		peer.mu.Lock()
		for socket := range peer.sockets {
			socket.Close()
		}
		for _, cancel := range peer.cancel {
			cancel()
		}
		peer.mu.Unlock()
		peer.workers.Wait()
	})
	return peer
}

func (peer *protocolPeer) options() Settings {
	input := peerSettings()
	_, port, _ := net.SplitHostPort(peer.listener.Addr().String())
	number, _ := strconv.Atoi(port)
	input.Port = uint16(number)
	input.Timeout = time.Second
	input.CloseTimeout = 100 * time.Millisecond
	if peer.tls != nil {
		input.Plaintext = false
		input.RootCAPEM = peer.roots
		input.ServerName = "fixture.invalid"
	}
	return input
}

func authenticatePeer(backend *pgproto3.Backend) bool {
	_ = backend.SetAuthType(pgproto3.AuthTypeSASL)
	backend.Send(&pgproto3.AuthenticationSASL{AuthMechanisms: []string{"SCRAM-SHA-256"}})
	if backend.Flush() != nil {
		return false
	}
	message, err := backend.Receive()
	first, ok := message.(*pgproto3.SASLInitialResponse)
	if err != nil || !ok || first.AuthMechanism != "SCRAM-SHA-256" {
		return false
	}
	_, bare, ok := strings.Cut(string(first.Data), ",,")
	if !ok {
		return false
	}
	_, nonce, ok := strings.Cut(bare, ",r=")
	if !ok {
		return false
	}
	salt := []byte("bounded-protocol-peer-salt")
	serverFirst := "r=" + nonce + "server,s=" + base64.StdEncoding.EncodeToString(salt) + ",i=4096"
	_ = backend.SetAuthType(pgproto3.AuthTypeSASLContinue)
	backend.Send(&pgproto3.AuthenticationSASLContinue{Data: []byte(serverFirst)})
	if backend.Flush() != nil {
		return false
	}
	message, err = backend.Receive()
	final, ok := message.(*pgproto3.SASLResponse)
	if err != nil || !ok {
		return false
	}
	withoutProof, proofText, ok := strings.Cut(string(final.Data), ",p=")
	if !ok {
		return false
	}
	auth := bare + "," + serverFirst + "," + withoutProof
	salted, err := pbkdf2.Key(sha256.New, "credential-canary", salt, 4096, 32)
	if err != nil {
		return false
	}
	mac := func(key []byte, value string) []byte {
		hash := hmac.New(sha256.New, key)
		hash.Write([]byte(value))
		return hash.Sum(nil)
	}
	clientKey := mac(salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	signature := mac(stored[:], auth)
	proof, err := base64.StdEncoding.DecodeString(proofText)
	if err != nil || len(proof) != len(clientKey) {
		return false
	}
	for index := range proof {
		proof[index] ^= signature[index]
	}
	if !hmac.Equal(proof, clientKey) {
		backend.Send(&pgproto3.ErrorResponse{Severity: "FATAL", Code: "28P01", Message: "authentication-canary"})
		backend.Flush()
		return false
	}
	backend.Send(&pgproto3.AuthenticationSASLFinal{Data: []byte("v=" + base64.StdEncoding.EncodeToString(mac(mac(salted, "Server Key"), auth)))})
	return backend.Flush() == nil
}

func (peer *protocolPeer) serve(socket net.Conn) {
	backend := pgproto3.NewBackend(socket, socket)
	message, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}
	if _, ok := message.(*pgproto3.SSLRequest); ok {
		if peer.tls == nil {
			socket.Write([]byte("N"))
			return
		}
		if _, err := socket.Write([]byte("S")); err != nil {
			return
		}
		socket = tls.Server(socket, peer.tls)
		backend = pgproto3.NewBackend(socket, socket)
		message, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}
	if cancel, ok := message.(*pgproto3.CancelRequest); ok {
		peer.mu.Lock()
		if cancelFunc := peer.cancel[cancel.ProcessID]; cancelFunc != nil {
			cancelFunc()
		}
		peer.mu.Unlock()
		return
	}
	if _, ok := message.(*pgproto3.StartupMessage); !ok {
		return
	}
	if peer.blockStartup.Load() {
		select {
		case peer.entered <- struct{}{}:
		default:
		}
		buffer := make([]byte, 1)
		socket.Read(buffer)
		return
	}
	if !authenticatePeer(backend) {
		return
	}
	pid := uint32(peer.connects.Add(1))
	ctx, cancel := context.WithCancel(context.Background())
	peer.mu.Lock()
	peer.cancel[pid] = cancel
	peer.mu.Unlock()
	defer func() { cancel(); peer.mu.Lock(); delete(peer.cancel, pid); peer.mu.Unlock() }()
	status := byte('I')
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: pid, SecretKey: []byte{1, 2, 3, 4}})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "18.6"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
	if backend.Flush() != nil {
		return
	}
	var sql string
	var parameters [][]byte
	statements := make(map[string]string)
	var savepoints []string
	fields := func() {
		if sql == "DISCARD ALL" || sql == "-- ping" || strings.HasPrefix(sql, "COPY ") ||
			strings.HasPrefix(sql, "SAVEPOINT ") || strings.HasPrefix(sql, "RELEASE SAVEPOINT ") || strings.HasPrefix(sql, "ROLLBACK TO SAVEPOINT ") {
			backend.Send(&pgproto3.NoData{})
			return
		}
		count := 1
		if sql == "SELECT cells" {
			count = 3
		}
		if sql == "SELECT wide_nulls" || sql == "SELECT columns64" {
			count = 64
		}
		if sql == "SELECT columns65" {
			count = 65
		}
		if strings.HasPrefix(sql, "INSERT") || strings.HasPrefix(sql, "UPDATE") || strings.HasPrefix(sql, "DELETE") {
			backend.Send(&pgproto3.NoData{})
			return
		}
		values := make([]pgproto3.FieldDescription, count)
		for index := range values {
			values[index] = pgproto3.FieldDescription{Name: []byte("value"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1}
		}
		backend.Send(&pgproto3.RowDescription{Fields: values})
	}
	execute := func() {
		if strings.HasPrefix(sql, "SAVEPOINT ") || strings.HasPrefix(sql, "RELEASE SAVEPOINT ") || strings.HasPrefix(sql, "ROLLBACK TO SAVEPOINT ") {
			tag := "SAVEPOINT"
			parts := strings.Fields(sql)
			name := parts[len(parts)-1]
			if strings.HasPrefix(sql, "SAVEPOINT ") {
				savepoints = append(savepoints, name)
			} else {
				index := -1
				for offset := len(savepoints) - 1; offset >= 0; offset-- {
					if savepoints[offset] == name {
						index = offset
						break
					}
				}
				if index < 0 {
					backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "3B001", Message: "savepoint-not-found"})
					status = 'E'
					return
				}
				if strings.HasPrefix(sql, "RELEASE") {
					savepoints = savepoints[:index]
				} else {
					savepoints = savepoints[:index+1]
				}
			}
			if strings.HasPrefix(sql, "RELEASE") {
				tag = "RELEASE"
			}
			if strings.HasPrefix(sql, "ROLLBACK") {
				tag = "ROLLBACK"
				status = 'T'
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
			return
		}
		if sql == "DISCARD ALL" {
			if peer.failReset.Load() {
				backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42501", Message: "reset-cause-canary"})
				return
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("DISCARD ALL")})
			return
		}
		if sql == "-- ping" {
			backend.Send(&pgproto3.EmptyQueryResponse{})
			return
		}
		peer.queries.Add(1)
		switch sql {
		case "COPY fixture TO STDOUT":
			backend.Send(&pgproto3.CopyOutResponse{ColumnFormatCodes: []uint16{0}})
			backend.Send(&pgproto3.CopyData{Data: []byte("copy-canary\n")})
			backend.Send(&pgproto3.CopyDone{})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")})
			return
		case "SELECT wide_nulls":
			for range 8192 {
				backend.Send(&pgproto3.DataRow{Values: make([][]byte, 64)})
			}
		case "SELECT columns64", "SELECT columns65":
			count := 64
			if sql == "SELECT columns65" {
				count = 65
			}
			backend.Send(&pgproto3.DataRow{Values: make([][]byte, count)})
		case "SELECT cells":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{nil, {}, []byte("value-canary")}})
		case "SELECT empty":
		case "SELECT partial":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("first")}})
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "native-cause-canary", Detail: "private-detail-canary"})
			if status == 'T' {
				status = 'E'
			}
			return
		case "SELECT late":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("first")}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "23514", Message: "late-cause-canary"})
			return
		case "SELECT over":
			for range 3 {
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("row")}})
			}
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "22012", Message: "drain-cause-canary"})
			return
		case "SELECT large":
			backend.Send(&pgproto3.DataRow{Values: [][]byte{bytes.Repeat([]byte("x"), 2048)}})
		case "SELECT wait":
			select {
			case peer.entered <- struct{}{}:
			default:
			}
			select {
			case <-ctx.Done():
			case <-peer.stop:
			}
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "canceled"})
			return
		case "SELECT $1::text":
			var value []byte
			if len(parameters) > 0 {
				value = parameters[0]
			}
			backend.Send(&pgproto3.DataRow{Values: [][]byte{value}})
		default:
			if !strings.HasPrefix(sql, "INSERT") && !strings.HasPrefix(sql, "UPDATE") && !strings.HasPrefix(sql, "DELETE") {
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte("one")}})
			}
		}
		tag := "SELECT 1"
		if sql == "SELECT empty" {
			tag = "SELECT 0"
		}
		if strings.HasPrefix(sql, "INSERT") {
			tag = "INSERT 0 1"
		}
		backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
	}
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		switch message := message.(type) {
		case *pgproto3.Terminate:
			return
		case *pgproto3.Parse:
			sql = message.Query
			statements[message.Name] = sql
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Bind:
			sql = statements[message.PreparedStatement]
			parameters = make([][]byte, len(message.Parameters))
			for index, value := range message.Parameters {
				if value != nil {
					parameters[index] = append([]byte{}, value...)
				}
			}
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Describe:
			if message.ObjectType == 'S' {
				sql = statements[message.Name]
				count := strings.Count(sql, "$")
				backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: make([]uint32, count)})
			}
			fields()
		case *pgproto3.Close:
			if peer.failDeallocate.Load() {
				backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "57014", Message: "deallocate-canary"})
				if status == 'T' {
					status = 'E'
				}
			} else {
				delete(statements, message.Name)
				backend.Send(&pgproto3.CloseComplete{})
			}
		case *pgproto3.Execute:
			execute()
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		case *pgproto3.Query:
			sql = message.String
			switch {
			case strings.HasPrefix(sql, "begin"):
				status = 'T'
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case sql == "commit":
				peer.commits.Add(1)
				if peer.dropCommit.Load() {
					return
				}
				tag := "COMMIT"
				if status == 'E' {
					tag = "ROLLBACK"
				}
				status = 'I'
				savepoints = nil
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte(tag)})
			case sql == "rollback":
				peer.rollbacks.Add(1)
				if peer.dropRollback.Load() {
					return
				}
				status = 'I'
				savepoints = nil
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
			default:
				fields()
				execute()
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		default:
			return
		}
		if backend.Flush() != nil {
			return
		}
	}
}

func peerSettings() Settings {
	return Settings{Name: "fixture", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture", Password: "credential-canary", Plaintext: true, ParserHome: os.Getenv("HOME"), MaxConnections: 1, MaxRows: 8, MaxResultBytes: 4096, MaxMessageBytes: 4096, Timeout: time.Second, CloseTimeout: time.Second}
}
