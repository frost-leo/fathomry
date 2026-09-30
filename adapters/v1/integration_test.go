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

package adapters_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type projectSettings struct {
	Endpoint    string `json:"endpoint"`
	Application struct {
		I18n i18n.Preferences `json:"i18n"`
	} `json:"application"`
}

func publish(t testing.TB, store settings.Store[projectSettings], endpoint, locale string) settings.View {
	t.Helper()
	config := projectSettings{Endpoint: endpoint}
	config.Application.I18n.Locale = locale
	snapshot, err := settings.New(config, func(value projectSettings) projectSettings { return value })
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Publish(snapshot); err != nil {
		t.Fatal(err)
	}
	view, err := store.Reader().Capture()
	if err != nil {
		t.Fatal(err)
	}
	return view
}
func operationComponent() i18n.Component {
	return i18n.Component{Module: "fathomry", Name: "operation", BaseLocale: "en",
		Resources: adapters.Resources(), Directory: "resources", Definitions: adapters.Definitions()}
}

type httpAccess struct {
	client   *http.Client
	endpoint string
	closed   *atomic.Bool
}
type httpFacts struct {
	Status int
	Body   string
}
type statusError struct{ status int }

func (err *statusError) Error() string { return "private-http-status-canary" }

func TestResourceHTTPComposition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started, finish := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	releaseBody := func() { unblock.Do(func() { close(finish) }) }
	first := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		close(started)
		select {
		case <-finish:
			_, _ = io.WriteString(writer, "first")
		case <-request.Context().Done():
		}
	}))
	defer first.Close()
	defer releaseBody()
	second := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(writer, "second")
	}))
	defer second.Close()
	scope, err := resource.New(ctx, resource.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseBody()
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := scope.Close(wait); err != nil {
			t.Error(err)
		}
	}()
	cleaned := make(chan string, 2)
	var firstClosed atomic.Bool
	ref, err := resource.Bind(scope, resource.Binding[string, httpAccess]{
		Name: "http", Policy: resource.Follow,
		Select: func(view settings.View) (string, error) {
			snapshot, err := settings.As[projectSettings](view)
			if err != nil {
				return "", err
			}
			value, err := snapshot.ValueCopy()
			return value.Endpoint, err
		},
		Clone: func(value string) string { return value },
		Equal: func(left, right string) bool { return left == right },
		Build: func(_ context.Context, endpoint string) (*resource.Instance[httpAccess], error) {
			transport := &http.Transport{}
			closed := new(atomic.Bool)
			if endpoint == first.URL {
				closed = &firstClosed
			}
			return &resource.Instance[httpAccess]{
				Value: httpAccess{client: &http.Client{Transport: transport}, endpoint: endpoint, closed: closed},
				Release: func(context.Context) resource.ReleaseResult {
					closed.Store(true)
					transport.CloseIdleConnections()
					cleaned <- endpoint
					return resource.ReleaseResult{Complete: true}
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := adapters.New(ctx, adapters.Options{MaxActive: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		releaseBody()
		wait, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := owner.Close(wait); err != nil {
			t.Error(err)
		}
	}()
	inbox, _ := adapters.NewInbox[httpFacts](adapters.EvidenceOptions{Capacity: 2})
	endpoint, err := adapters.Bind(owner, adapters.Declaration[httpFacts]{Copy: func(value httpFacts) httpFacts { return value }, Evidence: inbox})
	if err != nil {
		t.Fatal(err)
	}
	store := settings.NewStore[projectSettings]()
	apply := func(address, locale string) {
		t.Helper()
		update, err := scope.Apply(ctx, publish(t, store, address, locale))
		if err != nil {
			t.Fatal(err)
		}
		if err := update.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	read := func(call *adapters.Call[httpFacts], access httpAccess) {
		input, err := http.NewRequestWithContext(call.Context(), http.MethodGet, access.endpoint, nil)
		if err != nil {
			_ = call.Resolve(adapters.Outcome[httpFacts]{Primary: err})
			return
		}
		response, err := access.client.Do(input)
		if err != nil {
			_ = call.Resolve(adapters.Outcome[httpFacts]{Primary: err})
			return
		}
		guard, err := call.Hold()
		if err != nil {
			_ = call.Resolve(adapters.Outcome[httpFacts]{Primary: err, Cleanup: response.Body.Close()})
			return
		}
		go func() {
			defer guard.Release()
			body, readErr := io.ReadAll(io.LimitReader(response.Body, 1024))
			closeErr := response.Body.Close()
			if access.closed.Load() {
				t.Error("instance retired before body completion")
			}
			if response.StatusCode != http.StatusOK {
				readErr = errors.Join(readErr, &statusError{status: response.StatusCode})
			}
			_ = call.Resolve(adapters.Outcome[httpFacts]{Value: httpFacts{Status: response.StatusCode, Body: string(body)},
				Present: true, Primary: readErr, Cleanup: closeErr})
		}()
	}
	request := adapters.Request{Operation: "http.read", WorkBytes: 1024, EvidenceBytes: 1024}
	apply(first.URL, "en")
	previous, err := adapters.StartUsing(ctx, endpoint, ref, request, read)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first request did not start")
	}
	wait, stop := context.WithCancel(ctx)
	stop()
	if _, err := previous.Wait(wait); !errors.Is(err, adapters.ErrWait) {
		t.Fatal("waiting declared active body complete")
	}
	apply(second.URL, "zh-CN")
	if firstClosed.Load() {
		t.Fatal("replacement retired borrowed generation")
	}
	current, err := adapters.Using(ctx, endpoint, ref, request, read)
	if err != nil {
		t.Fatal(err)
	}
	next, err := current.WaitReleased(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, present := next.ValueCopy()
	if !present || data.Status != http.StatusServiceUnavailable || data.Body != "second" {
		t.Fatal("new operation used wrong generation")
	}
	var native *statusError
	if !errors.As(next.Err(), &native) || native.status != http.StatusServiceUnavailable {
		t.Fatal("native status evidence erased")
	}
	before, _ := previous.Snapshot()
	if before.Info().Source.Generation == 0 || before.Info().Source.Generation == next.Info().Source.Generation || before.Info().Released {
		t.Fatal("borrow provenance changed")
	}
	catalog, err := i18n.Prepare(append(i18n.CoreComponents(), operationComponent())...)
	if err != nil {
		t.Fatal(err)
	}
	presenter, _ := i18n.NewPresenter(catalog)
	presenter, err = presenter.WithSettings(store.Reader())
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	slog.New(slog.NewJSONHandler(&output, nil)).ErrorContext(ctx, "http.operation.failed", "error", presenter.Present(next.Err()))
	if !strings.Contains(output.String(), `"locale":"zh-CN"`) || strings.Contains(output.String(), "private-http-status-canary") {
		t.Fatal("log boundary lost locale/privacy")
	}
	releaseBody()
	old, err := previous.WaitReleased(ctx)
	if err != nil || old.Err() != nil {
		t.Fatal("old operation lost its body", err)
	}
	if value, present := old.ValueCopy(); !present || value.Body != "first" {
		t.Fatal("old operation retargeted")
	}
	select {
	case address := <-cleaned:
		if address != first.URL {
			t.Fatal("wrong generation cleaned")
		}
	case <-ctx.Done():
		t.Fatal("retired instance not cleaned after release")
	}
	for range 2 {
		if err := inbox.DeliverOne(ctx, func(_ context.Context, value adapters.Snapshot[httpFacts]) error {
			if !value.Info().Released {
				return errors.New("unreleased evidence delivered")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := owner.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPresentationComposition(t *testing.T) {
	definitions := adapters.Definitions()
	catalog, err := i18n.Prepare(append(i18n.CoreComponents(), operationComponent())...)
	if err != nil {
		t.Fatal(err)
	}
	store := settings.NewStore[projectSettings]()
	publish(t, store, "", "en")
	presenter, _ := i18n.NewPresenter(catalog)
	presenter, err = presenter.WithSettings(store.Reader())
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range definitions {
		t.Run(string(definition.Identifier), func(t *testing.T) {
			publish(t, store, "", "en")
			native := &statusError{status: 503}
			original, err := failure.New(definition, failure.Location{Operation: "example"}, native)
			if err != nil {
				t.Fatal(err)
			}
			english := presenter.Present(original).(*i18n.Presented)
			if english.Issue() != nil || english.Info().Message.Text != definition.Message {
				t.Fatal("English resources differ from definition")
			}
			publish(t, store, "", "zh-CN")
			chinese := presenter.Present(original).(*i18n.Presented)
			if chinese.Issue() != nil || chinese.Info().Message.Locale != "zh-CN" || chinese.Info().Message.Text == definition.Message ||
				english.Info().Message.Locale != "en" || !errors.Is(chinese, definition.Code) || !errors.Is(chinese, native) {
				t.Fatal("locale change rewrote error identity or history")
			}
			explanation, found, err := catalog.Explain(definition.Code, "zh-CN")
			if err != nil || !found || explanation.Message.Text != chinese.Info().Message.Text {
				t.Fatal("offline code lookup differs")
			}
			var output bytes.Buffer
			slog.New(slog.NewJSONHandler(&output, nil)).Error("operation.failed", "error", chinese)
			var record struct {
				Error struct{ Code, Identifier, Message, Locale string }
			}
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record.Error.Code != definition.Code.String() || record.Error.Locale != "zh-CN" ||
				record.Error.Identifier != string(definition.Identifier) || record.Error.Message != chinese.Info().Message.Text ||
				strings.Contains(output.String(), "canary") {
				t.Fatal("structured error log differs from frozen presentation")
			}
			publish(t, store, "", "")
			fallback := presenter.Present(original).(*i18n.Presented)
			if !errors.Is(fallback.Issue(), i18n.ErrPreferences) || !errors.Is(fallback, native) ||
				fallback.Info().Message.Text != definition.Message {
				t.Fatal("presentation failure replaced operation failure")
			}
		})
	}
	definitions[0].Message = "modified caller copy"
	if adapters.Definitions()[0].Message == definitions[0].Message {
		t.Fatal("definitions are mutable shared storage")
	}
}

func TestIndependentModule(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	notice, err := os.ReadFile(filepath.Join(root, ".github/LICENSE_HEADER"))
	if err != nil {
		t.Fatal(err)
	}
	header := "/**\n * " + strings.ReplaceAll(strings.TrimSpace(string(notice)), "\n", "\n * ") + "\n */\n"
	header = strings.ReplaceAll(header, "\n * \n", "\n *\n")
	write := func(name, content string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", fmt.Sprintf("module example.org/adapters-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root)))
	source, err := os.ReadFile("testdata/consumer/consumer_test.go")
	if err != nil {
		t.Fatal(err)
	}
	write("consumer_test.go", string(source))
	run := func(arguments ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "go", arguments...)
		command.Dir = directory
		command.Env = append(os.Environ(), "GOWORK=off", "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off")
		result, err := command.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatal("independent command exceeded its bound")
		}
		return result, err
	}
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"test", "-mod=readonly", "-race", "-count=1", "./..."}} {
		if output, err := run(args...); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
	}
	output, err := run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/adapters/v1")
	expected := "github.com/frost-leo/fathomry/failure/v1\ngithub.com/frost-leo/fathomry/settings/v1\ngithub.com/frost-leo/fathomry/resource/v1\ngithub.com/frost-leo/fathomry/adapters/v1"
	if err != nil || strings.TrimSpace(string(output)) != expected {
		t.Fatalf("unexpected public operation dependencies: %v\n%s", err, output)
	}
	for _, handle := range []string{"Endpoint", "Call", "Receipt", "Snapshot", "Inbox", "Delivery"} {
		for _, star := range []string{"", "*"} {
			write("conversion_test.go", header+fmt.Sprintf("package consumer\nimport a \"github.com/frost-leo/fathomry/adapters/v1\"\nfunc forbidden(value %[1]sa.%[2]s[struct{Value string `json:\"a\"`}]) %[1]sa.%[2]s[struct{Value string `json:\"b\"`}] { return (%[1]sa.%[2]s[struct{Value string `json:\"b\"`}])(value) }\n", star, handle))
			if output, err := run("test", "-run", "^$", "."); err == nil || !strings.Contains(string(output), "cannot convert") {
				t.Fatalf("generic handle conversion admitted: %v\n%s", err, output)
			}
		}
	}
	write("conversion_test.go", header+"package consumer\nimport a \"github.com/frost-leo/fathomry/adapters/v1\"\ntype Alias = struct{ Value string `json:\"a\"` }\nfunc allowed(value a.Receipt[Alias]) a.Receipt[struct{ Value string `json:\"a\"` }] { return value }\n")
	for _, handle := range []string{"Receipt[int]", "Snapshot[int]", "Scope", "Delivery[int]"} {
		write("authority_test.go", header+fmt.Sprintf("package consumer\nimport ( \"context\"; a \"github.com/frost-leo/fathomry/adapters/v1\" )\nfunc forbidden(value a.%s) { value.Close(context.Background()) }\n", handle))
		if output, err := run("test", "-run", "^$", "."); err == nil || !strings.Contains(string(output), "has no field or method Close") {
			t.Fatalf("read-only/child handle acquired shutdown authority: %v\n%s", err, output)
		}
	}
	write("authority_test.go", header+"package consumer\n")
	if output, err := run("test", "-mod=readonly", "-race", "-count=1", "./..."); err != nil {
		t.Fatalf("genuine aliases failed: %v\n%s", err, output)
	}
}
