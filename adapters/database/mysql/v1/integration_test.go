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

package mysql

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"debug/buildinfo"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"io"
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
)

func TestSharedSourceNativeCapacity(t *testing.T) {
	peer := newPeer(t, false, false)
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
	peer := newPeer(t, true, false)
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
	if _, err = empty.First(); !errors.Is(err, sql.ErrNoRows) {
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
	statement, err := client.Prepare(setup, "SELECT ?")
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
	prepared, err := transaction.Prepare(ctx, "SELECT ?")
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
	firstPeer, secondPeer := newPeer(t, true, false), newPeer(t, true, false)
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
	transaction, err := follow.Begin(context.Background(), TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	statement, err := follow.Prepare(context.Background(), "SELECT ?")
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
	catalog, err := i18n.Prepare(i18n.Component{Module: "fathomry", Name: "database_mysql", BaseLocale: "en", Resources: Resources(), Directory: "resources", Definitions: Definitions()})
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
		"go.mod":  []byte(fmt.Sprintf("module example.org/mysql-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root))),
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
		if strings.HasPrefix(path, "github.com/frost-leo/fathomry/adapters/database/postgres/") ||
			strings.HasPrefix(path, "github.com/frost-leo/fathomry/internal/database/pgx/") ||
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
		if module.Path == "github.com/go-sql-driver/mysql" {
			found = module.Version == "v1.10.1" && module.Replace == nil && module.Sum != ""
		}
	}
	if !found {
		t.Fatal("consumer binary does not use the selected native SDK")
	}
	peer := newPeer(t, true, false)
	input, err := json.Marshal(peer.options())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Stdin = bytes.NewReader(input)
	if output, err := command.CombinedOutput(); err != nil || string(output) != "mysql public consumer passed\n" {
		t.Fatalf("independent protocol consumer: %v\n%s", err, output)
	}
	for _, check := range []struct{ name, source, want string }{
		{"client_shutdown", "var _ = (*p.Client).Close", "has no field or method Close"},
		{"raw_connection", "var _ = (*p.Client).Conn", "has no field or method Conn"},
		{"handle_ownership", "var _ = p.Owner(p.Handle{})", "cannot convert"},
	} {
		t.Run(check.name, func(t *testing.T) {
			content := notice + "package main\nimport p \"github.com/frost-leo/fathomry/adapters/database/mysql/v1\"\n" + check.source + "\n"
			if err := os.WriteFile(filepath.Join(directory, "forbidden.go"), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if output, err := run("build", "-mod=readonly", "-o", filepath.Join(directory, "forbidden"), "."); err == nil || !strings.Contains(string(output), check.want) {
				t.Fatalf("authority boundary not enforced: %v\n%s", err, output)
			}
		})
	}
	private := "github.com/frost-leo/fathomry/internal/database/mysql/v1"
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
			peer := newPeer(t, false, false)
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

// A bounded native-protocol peer, not MySQL or an InnoDB/durability oracle.
type protocolPeer struct {
	listener      net.Listener
	trust         string
	tls           *tls.Config
	key           *rsa.PrivateKey
	fullAuth      bool
	nativeAuth    bool
	emptyPassword bool
	mu            sync.Mutex
	sockets       map[net.Conn]struct{}
	workers       sync.WaitGroup
	stopped       bool
	entered       chan struct{}
	queries       atomic.Int64
	commits       atomic.Int64
	rollbacks     atomic.Int64
	secure        atomic.Int64
	fileCalls     atomic.Int64
	upload        string
	dropCommit    atomic.Bool
}

func packetRead(conn net.Conn) ([]byte, byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(conn, h[:]); err != nil {
		return nil, 0, err
	}
	size := int(h[0]) | int(h[1])<<8 | int(h[2])<<16
	if size > 2<<20 {
		return nil, 0, errors.New("fixture packet limit")
	}
	body := make([]byte, size)
	_, err := io.ReadFull(conn, body)
	return body, h[3], err
}

func packetSend(conn net.Conn, seq byte, body []byte) error {
	packet := make([]byte, 4+len(body))
	packet[0], packet[1], packet[2], packet[3] = byte(len(body)), byte(len(body)>>8), byte(len(body)>>16), seq
	copy(packet[4:], body)
	_, err := conn.Write(packet)
	return err
}

func encoded(value string) []byte {
	if len(value) < 251 {
		return append([]byte{byte(len(value))}, value...)
	}
	if len(value) <= 65535 {
		return append([]byte{0xfc, byte(len(value)), byte(len(value) >> 8)}, value...)
	}
	return append([]byte{0xfd, byte(len(value)), byte(len(value) >> 8), byte(len(value) >> 16)}, value...)
}

func greeting(secure bool) []byte {
	flags := uint32(1 | 4 | 8 | 128 | 512 | 8192 | 32768 | 1<<17 | 1<<19 | 1<<20 | 1<<21 | 1<<24)
	if secure {
		flags |= 1 << 11
	}
	data := append([]byte{10}, []byte("8.4.11-protocol-peer\x00")...)
	data = append(data, 1, 0, 0, 0)
	data = append(data, []byte("12345678")...)
	data = append(data, 0, byte(flags), byte(flags>>8), 45, 2, 0, byte(flags>>16), byte(flags>>24), 21)
	data = append(data, make([]byte, 10)...)
	return append(data, []byte("abcdefghijkl\x00caching_sha2_password\x00")...)
}

func newPeer(t testing.TB, secure, fullAuth bool) *protocolPeer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &protocolPeer{listener: listener, sockets: make(map[net.Conn]struct{}), entered: make(chan struct{}, 8), fullAuth: fullAuth}
	if secure {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(25), DNSNames: []string{"mysql.fixture.invalid"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
		if err != nil {
			t.Fatal(err)
		}
		p.trust = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
		p.tls = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}}
	}
	if fullAuth {
		p.key, err = rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
	}
	p.workers.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.stopped {
				p.mu.Unlock()
				_ = conn.Close()
				return
			}
			p.sockets[conn] = struct{}{}
			p.mu.Unlock()
			p.workers.Go(func() {
				defer conn.Close()
				defer func() { p.mu.Lock(); delete(p.sockets, conn); p.mu.Unlock() }()
				p.serve(conn)
			})
		}
	})
	t.Cleanup(func() {
		p.mu.Lock()
		p.stopped = true
		_ = listener.Close()
		for conn := range p.sockets {
			_ = conn.Close()
		}
		p.mu.Unlock()
		p.workers.Wait()
	})
	return p
}

func (p *protocolPeer) options() Settings {
	_, port, _ := net.SplitHostPort(p.listener.Addr().String())
	number, _ := strconv.Atoi(port)
	s := peerSettings()
	s.Port = uint16(number)
	if p.tls == nil {
		s.Plaintext = true
	} else {
		s.Plaintext = false
		s.RootCAPEM = p.trust
		s.ServerName = "mysql.fixture.invalid"
	}
	return s
}

func (p *protocolPeer) authenticate(conn net.Conn) (net.Conn, bool) {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if packetSend(conn, 0, greeting(p.tls != nil)) != nil {
		return conn, false
	}
	auth, seq, err := packetRead(conn)
	if err != nil || seq != 1 {
		return conn, false
	}
	if len(auth) == 32 {
		if p.tls == nil {
			return conn, false
		}
		secure := tls.Server(conn, p.tls)
		if secure.HandshakeContext(context.Background()) != nil {
			return conn, false
		}
		conn = secure
		p.secure.Add(1)
		auth, seq, err = packetRead(conn)
		if err != nil || seq != 2 || len(auth) < 32 || binary.LittleEndian.Uint32(auth)&(1<<11) == 0 {
			return conn, false
		}
	}
	if p.nativeAuth {
		request := append([]byte{0xfe}, []byte("mysql_native_password\x00abcdefghijkl12345678\x00")...)
		if packetSend(conn, seq+1, request) != nil {
			return conn, false
		}
		response, next, err := packetRead(conn)
		size := sha1.Size
		if p.emptyPassword {
			size = 0
		}
		if err != nil || next != seq+2 || len(response) != size {
			return conn, false
		}
		seq = next
	}
	if p.fullAuth {
		seq++
		if packetSend(conn, seq, []byte{1, 4}) != nil {
			return conn, false
		}
		request, requestSeq, err := packetRead(conn)
		if _, encrypted := conn.(*tls.Conn); encrypted {
			if err != nil || requestSeq != seq+1 || string(request) != "credential-canary\x00" {
				return conn, false
			}
			return conn, packetSend(conn, requestSeq+1, []byte{0, 0, 0, 2, 0, 0, 0}) == nil
		}
		if err != nil || len(request) != 1 || request[0] != 2 || requestSeq != seq+1 {
			return conn, false
		}
		der, err := x509.MarshalPKIXPublicKey(&p.key.PublicKey)
		if err != nil {
			return conn, false
		}
		seq = requestSeq + 1
		if packetSend(conn, seq, append([]byte{1}, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})...)) != nil {
			return conn, false
		}
		encrypted, encryptedSeq, err := packetRead(conn)
		if err != nil || encryptedSeq != seq+1 {
			return conn, false
		}
		decrypted, err := rsa.DecryptOAEP(sha1.New(), rand.Reader, p.key, encrypted, nil)
		if err != nil {
			return conn, false
		}
		seed := []byte("12345678abcdefghijkl")
		for index := range decrypted {
			decrypted[index] ^= seed[index%len(seed)]
		}
		if string(decrypted) != "credential-canary\x00" {
			return conn, false
		}
		seq = encryptedSeq
	}
	return conn, packetSend(conn, seq+1, []byte{0, 0, 0, 2, 0, 0, 0}) == nil
}

func columnPacket(name string, kind byte, flags uint16) []byte {
	var data []byte
	for _, v := range []string{"def", "fixture", "", "", name, name} {
		data = append(data, encoded(v)...)
	}
	return append(data, 12, 45, 0, 255, 255, 0, 0, kind, byte(flags), byte(flags>>8), 0, 0, 0)
}

type preparedQuery struct {
	query      string
	parameters int
}

func (p *protocolPeer) serve(socket net.Conn) {
	conn, ok := p.authenticate(socket)
	if !ok {
		return
	}
	statements := make(map[uint32]preparedQuery)
	var id uint32
	inTx := false
	for {
		body, _, err := packetRead(conn)
		if err != nil || len(body) == 0 {
			return
		}
		status := byte(2)
		if inTx {
			status = 3
		}
		switch body[0] {
		case 1:
			return
		case 14:
			if packetSend(conn, 1, []byte{0, 0, 0, status, 0, 0, 0}) != nil {
				return
			}
		case 3:
			query := string(body[1:])
			switch {
			case strings.HasPrefix(query, "SET "):
			case strings.HasPrefix(query, "START TRANSACTION"):
				inTx = true
				status = 3
			case query == "COMMIT":
				p.commits.Add(1)
				inTx = false
				status = 2
				if p.dropCommit.Load() {
					return
				}
			case query == "ROLLBACK":
				p.rollbacks.Add(1)
				inTx = false
				status = 2
			case query == "SELECT huge_header":
				_, _ = conn.Write([]byte{0, 0, 128, 1})
				_, _, _ = packetRead(conn)
				return
			case strings.HasPrefix(query, "SELECT") || strings.HasPrefix(query, "SHOW"):
				p.queries.Add(1)
				if !p.textResult(conn, query, status) {
					return
				}
				if query == "SELECT partial" {
					inTx = false
				}
				continue
			case strings.HasPrefix(query, "CREATE "), strings.HasPrefix(query, "SAVEPOINT "), strings.HasPrefix(query, "ROLLBACK TO "), strings.HasPrefix(query, "RELEASE SAVEPOINT "):
				p.queries.Add(1)
			default:
				return
			}
			if packetSend(conn, 1, []byte{0, 0, 0, status, 0, 0, 0}) != nil {
				return
			}
		case 0x16:
			query := string(body[1:])
			if query == "SELECT prepare_error" {
				if packetSend(conn, 1, append([]byte{255, 0x28, 4, '#', '4', '2', '0', '0', '0'}, []byte("native-prepare-error")...)) != nil {
					return
				}
				continue
			}
			id++
			params := strings.Count(query, "?")
			statements[id] = preparedQuery{query, params}
			columns := 0
			if strings.HasPrefix(query, "SELECT") {
				columns = 1
			}
			result := []byte{0, byte(id), byte(id >> 8), byte(id >> 16), byte(id >> 24), byte(columns), 0, byte(params), 0, 0, 0, 0}
			if packetSend(conn, 1, result) != nil {
				return
			}
			seq := byte(2)
			for range params {
				if packetSend(conn, seq, columnPacket("argument", 253, 0)) != nil {
					return
				}
				seq++
			}
			if params > 0 {
				if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
					return
				}
				seq++
			}
			for range columns {
				column := columnPacket("value", 253, 0)
				if query == "SELECT malformed_metadata" {
					column = []byte{0, 0, 0, 0, 0, 0}
				}
				if packetSend(conn, seq, column) != nil {
					return
				}
				seq++
			}
			if columns > 0 {
				if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
					return
				}
			}
		case 0x19:
			if len(body) < 5 {
				return
			}
			delete(statements, binary.LittleEndian.Uint32(body[1:]))
		case 0x18:
			p.entered <- struct{}{}
		case 0x17:
			if len(body) < 10 {
				return
			}
			selected, found := statements[binary.LittleEndian.Uint32(body[1:])]
			if !found {
				return
			}
			p.queries.Add(1)
			if selected.query == "SELECT wait" {
				p.entered <- struct{}{}
				_, _, _ = packetRead(conn)
				return
			}
			if selected.query == "SELECT infile" {
				if packetSend(conn, 1, append([]byte{0xfb}, p.upload...)) != nil {
					return
				}
				for {
					part, _, err := packetRead(conn)
					if err != nil {
						return
					}
					if len(part) > 0 {
						p.fileCalls.Add(1)
					} else {
						break
					}
				}
				return
			}
			if !strings.HasPrefix(selected.query, "SELECT") {
				if packetSend(conn, 1, []byte{0, 1, 7, status, 0, 0, 0}) != nil {
					return
				}
				continue
			}
			if selected.query == "SELECT huge_header" {
				_, _ = conn.Write([]byte{0, 0, 128, 1})
				_, _, _ = packetRead(conn)
				return
			}
			count := 1
			if selected.query == "SELECT cells" {
				count = 3
			}
			if selected.query == "SELECT wide" {
				count = 65
			}
			if packetSend(conn, 1, []byte{byte(count)}) != nil {
				return
			}
			seq := byte(2)
			kind := byte(253)
			flags := uint16(0)
			if selected.query == "SELECT unsigned" {
				kind = 8
				flags = 32
			}
			if selected.query == "SELECT decimal" {
				kind = 246
			}
			if selected.query == "SELECT zero_date" {
				kind = 12
			}
			if selected.query == "SELECT json" {
				kind = 245
			}
			for range count {
				if packetSend(conn, seq, columnPacket("value", kind, flags)) != nil {
					return
				}
				seq++
			}
			if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
				return
			}
			seq++
			if selected.query != "SELECT empty" {
				data := append([]byte{0, 0}, encoded("value-canary")...)
				switch selected.query {
				case "SELECT cells":
					data = append([]byte{0, 4, 0}, encoded("value-canary")...)
				case "SELECT unsigned":
					data = append([]byte{0, 0}, []byte{255, 255, 255, 255, 255, 255, 255, 255}...)
				case "SELECT decimal":
					data = append([]byte{0, 0}, encoded("12345678901234567890.00100")...)
				case "SELECT zero_date":
					data = []byte{0, 0, 0}
				case "SELECT json":
					data = append([]byte{0, 0}, encoded("{\"exact\":18446744073709551615}")...)
				case "SELECT large":
					data = append([]byte{0, 0}, encoded(strings.Repeat("x", 4096))...)
				case "SELECT ?":
					if selected.parameters != 1 || len(body) < 14 {
						return
					}
					if body[10]&1 != 0 {
						data = []byte{0, 4}
					} else {
						data = append([]byte{0, 0}, body[14:]...)
					}
				}
				if packetSend(conn, seq, data) != nil {
					return
				}
				seq++
				if selected.query == "SELECT over" {
					if packetSend(conn, seq, data) != nil {
						return
					}
					seq++
				}
				if selected.query == "SELECT partial" {
					inTx = false
					_ = packetSend(conn, seq, append([]byte{255, 0xbd, 4, '#', '4', '0', '0', '0', '1'}, []byte("native-error-canary")...))
					continue
				}
			}
			if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
				return
			}
		default:
			return
		}
	}
}

func (p *protocolPeer) textResult(conn net.Conn, query string, status byte) bool {
	if query == "SELECT wait" {
		p.entered <- struct{}{}
		_, _, _ = packetRead(conn)
		return false
	}
	if query == "SELECT infile" {
		if packetSend(conn, 1, append([]byte{0xfb}, p.upload...)) != nil {
			return false
		}
		for {
			body, seq, err := packetRead(conn)
			if err != nil {
				return false
			}
			if len(body) == 0 {
				return packetSend(conn, seq+1, []byte{0, 0, 0, status, 0, 0, 0}) == nil
			}
			p.fileCalls.Add(1)
		}
	}
	count := 1
	if query == "SELECT cells" {
		count = 3
	}
	if query == "SELECT wide" {
		count = 65
	}
	kind := byte(253)
	flags := uint16(0)
	switch query {
	case "SELECT unsigned":
		kind = 8
		flags = 32
	case "SELECT decimal":
		kind = 246
	case "SELECT zero_date":
		kind = 12
	case "SELECT json":
		kind = 245
	}
	if packetSend(conn, 1, []byte{byte(count)}) != nil {
		return false
	}
	seq := byte(2)
	for range count {
		if packetSend(conn, seq, columnPacket("value", kind, flags)) != nil {
			return false
		}
		seq++
	}
	if packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) != nil {
		return false
	}
	seq++
	if query != "SELECT empty" {
		data := encoded("value-canary")
		switch query {
		case "SELECT cells":
			data = append([]byte{0xfb, 0}, encoded("value-canary")...)
		case "SELECT unsigned":
			data = encoded("18446744073709551615")
		case "SELECT decimal":
			data = encoded("12345678901234567890.00100")
		case "SELECT zero_date":
			data = encoded("0000-00-00 00:00:00")
		case "SELECT json":
			data = encoded("{\"exact\":18446744073709551615}")
		case "SELECT large", "SELECT drain_error":
			data = encoded(strings.Repeat("x", 4096))
		case "SELECT duplicate":
			return packetSend(conn, seq, append([]byte{255, 0x26, 4, '#', '2', '3', '0', '0', '0'}, []byte("duplicate-canary")...)) == nil
		}
		if packetSend(conn, seq, data) != nil {
			return false
		}
		seq++
		if query == "SELECT over" {
			if packetSend(conn, seq, data) != nil {
				return false
			}
			seq++
		}
		if query == "SELECT partial" || query == "SELECT drain_error" {
			return packetSend(conn, seq, append([]byte{255, 0xbd, 4, '#', '4', '0', '0', '0', '1'}, []byte("native-error-canary")...)) == nil
		}
	}
	return packetSend(conn, seq, []byte{0xfe, 0, 0, status, 0}) == nil
}

func peerSettings() Settings {
	return Settings{Name: "fixture", Address: "127.0.0.1", Port: 1, Database: "fixture", User: "fixture", Password: "credential-canary", Plaintext: true, MaxConnections: 1, MaxRows: 8, MaxResultBytes: 4096, MaxPacketBytes: 4096, MaxResponseBytes: 16384, Timeout: time.Second, CloseTimeout: time.Second}
}
