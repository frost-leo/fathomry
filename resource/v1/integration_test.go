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

package resource_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type application struct {
	Endpoint string `json:"endpoint"`
	Locale   string `json:"locale"`
}
type httpInstance struct {
	client   *http.Client
	endpoint string
	closed   atomic.Bool
}

func TestPublicComposition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	first := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		close(started)
		select {
		case <-release:
		case <-request.Context().Done():
			return
		}
		_, _ = io.WriteString(writer, "first")
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(writer, "second") }))
	defer second.Close()

	components := append(i18n.CoreComponents(), i18n.Component{
		Module: "fathomry", Name: "resource", BaseLocale: "en", Resources: resource.Resources(),
		Directory: "resources", Definitions: resource.Definitions(),
	})
	catalog, err := i18n.Prepare(components...)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := resource.New(ctx, resource.Options{Name: "application"})
	if err != nil {
		t.Fatal(err)
	}
	defer scope.Close(context.Background())
	project := func(view settings.View) (application, error) {
		snapshot, err := settings.As[application](view)
		if err != nil {
			return application{}, err
		}
		return snapshot.ValueCopy()
	}
	cleaned := make(chan string, 2)
	var previous *httpInstance
	client, err := resource.Bind(scope, resource.Binding[string, *httpInstance]{
		Name: "business-http", Policy: resource.Follow,
		Select: func(view settings.View) (string, error) { value, err := project(view); return value.Endpoint, err },
		Clone:  func(value string) string { return value },
		Equal:  func(left, right string) bool { return left == right },
		Build: func(_ context.Context, endpoint string) (*resource.Instance[*httpInstance], error) {
			transport := &http.Transport{}
			value := &httpInstance{client: &http.Client{Transport: transport}, endpoint: endpoint}
			if endpoint == first.URL {
				previous = value
			}
			return &resource.Instance[*httpInstance]{Value: value, Release: func(context.Context) resource.ReleaseResult {
				transport.CloseIdleConnections()
				value.closed.Store(true)
				cleaned <- endpoint
				return resource.ReleaseResult{Complete: true}
			}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	presenter, err := resource.Bind(scope, resource.Binding[string, i18n.Presenter]{
		Name: "presentation", Policy: resource.Follow,
		Select: func(view settings.View) (string, error) { value, err := project(view); return value.Locale, err },
		Clone:  func(value string) string { return value },
		Equal:  func(left, right string) bool { return left == right },
		Build: func(_ context.Context, locale string) (*resource.Instance[i18n.Presenter], error) {
			value, err := i18n.NewPresenter(catalog)
			if err != nil {
				return nil, err
			}
			value, err = value.WithLocale(locale)
			if err != nil {
				return nil, err
			}
			return &resource.Instance[i18n.Presenter]{Value: value}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := settings.NewStore[application]()
	apply := func(value application) {
		t.Helper()
		snapshot, err := settings.New(value, func(value application) application { return value })
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
		update, err := scope.Apply(ctx, view)
		if err != nil {
			t.Fatal(err)
		}
		if err := update.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}
	request := func() (string, error) {
		lease, err := client.Acquire(ctx)
		if err != nil {
			return "", err
		}
		defer lease.Release()
		instance, err := lease.Value()
		if err != nil {
			return "", err
		}
		input, err := http.NewRequestWithContext(ctx, http.MethodGet, instance.endpoint, nil)
		if err != nil {
			return "", err
		}
		response, err := instance.client.Do(input)
		if err != nil {
			return "", err
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if instance.closed.Load() {
			return "", errors.New("transport cleaned during a borrow")
		}
		return string(raw), err
	}
	native := errors.New("private-operation-canary")
	original, err := failure.New(resource.Definitions()[0], failure.Location{Operation: "example"}, native)
	if err != nil {
		t.Fatal(err)
	}
	present := func() *i18n.Presented {
		t.Helper()
		lease, err := presenter.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer lease.Release()
		value, err := lease.Value()
		if err != nil {
			t.Fatal(err)
		}
		result, ok := value.Present(original).(*i18n.Presented)
		if !ok || result.Issue() != nil || !errors.Is(result, native) {
			t.Fatal("presentation lost original occurrence")
		}
		return result
	}

	apply(application{Endpoint: first.URL, Locale: "en"})
	english := present()
	oldResult := make(chan string, 1)
	oldError := make(chan error, 1)
	go func() { value, err := request(); oldResult <- value; oldError <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("old request did not start")
	}
	apply(application{Endpoint: second.URL, Locale: "zh-CN"})
	if previous.closed.Load() {
		t.Fatal("old HTTP request lost its transport")
	}
	if result, err := request(); err != nil || result != "second" {
		t.Fatal("new request did not select the new instance", err)
	}
	chinese := present()
	if chinese.Info().Message.Locale != "zh-CN" || english.Info().Message.Locale != "en" {
		t.Fatal("presentation generations mixed")
	}
	if explanation, found, err := catalog.Explain(resource.ErrOptions, "zh-CN"); err != nil || !found || explanation.Message.Text != chinese.Info().Message.Text {
		t.Fatal("offline atlas differs")
	}
	close(release)
	select {
	case result := <-oldResult:
		if result != "first" {
			t.Fatal("old operation retargeted")
		}
	case <-ctx.Done():
		t.Fatal("old request did not finish")
	}
	if err := <-oldError; err != nil {
		t.Fatal(err)
	}
	select {
	case endpoint := <-cleaned:
		if endpoint != first.URL {
			t.Fatal("wrong transport retired")
		}
	case <-ctx.Done():
		t.Fatal("old transport not retired")
	}
	if err := scope.Close(ctx); err != nil {
		t.Fatal(err)
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
	write("go.mod", fmt.Sprintf("module example.org/resource-consumer\n\ngo 1.27.0\nrequire github.com/frost-leo/fathomry v0.0.0\nreplace github.com/frost-leo/fathomry => %q\n", filepath.ToSlash(root)))
	source, err := os.ReadFile("testdata/consumer/consumer_test.go")
	if err != nil {
		t.Fatal(err)
	}
	write("consumer_test.go", string(source))
	run := func(arguments ...string) ([]byte, error) {
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
	for _, args := range [][]string{{"mod", "tidy"}, {"mod", "tidy", "-diff"}, {"test", "-race", "-count=1", "./..."}} {
		if output, err := run(args...); err != nil {
			t.Fatalf("%v: %v\n%s", args, err, output)
		}
	}
	output, err := run("list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "github.com/frost-leo/fathomry/resource/v1")
	expected := "github.com/frost-leo/fathomry/failure/v1\ngithub.com/frost-leo/fathomry/settings/v1\ngithub.com/frost-leo/fathomry/resource/v1"
	if err != nil || strings.TrimSpace(string(output)) != expected {
		t.Fatalf("unexpected resource dependencies: %v\n%s", err, output)
	}
	for _, handle := range []string{"Ref", "Lease"} {
		for _, star := range []string{"", "*"} {
			write("conversion_test.go", header+fmt.Sprintf("package consumer\nimport r \"github.com/frost-leo/fathomry/resource/v1\"\nfunc forbidden(value %[1]sr.%[2]s[struct{Value string `json:\"a\"`}]) %[1]sr.%[2]s[struct{Value string `json:\"b\"`}] { return (%[1]sr.%[2]s[struct{Value string `json:\"b\"`}])(value) }\n", star, handle))
			if output, err := run("test", "-run", "^$", "."); err == nil || !strings.Contains(string(output), "cannot convert") {
				t.Fatalf("generic handle conversion admitted: %v\n%s", err, output)
			}
		}
	}
	write("conversion_test.go", header+"package consumer\nimport r \"github.com/frost-leo/fathomry/resource/v1\"\ntype Alias = struct{ Value string `json:\"a\"` }\nfunc allowed(value r.Ref[Alias]) r.Ref[struct{ Value string `json:\"a\"` }] { return value }\n")
	if output, err := run("test", "-race", "-count=1", "./..."); err != nil {
		t.Fatalf("genuine aliases failed: %v\n%s", err, output)
	}
	write("authority_test.go", header+"package consumer\nimport ( \"context\"; r \"github.com/frost-leo/fathomry/resource/v1\" )\nfunc forbidden(value r.Ref[int]) { value.Close(context.Background()) }\n")
	if output, err := run("test", "-run", "^$", "."); err == nil || !strings.Contains(string(output), "has no field or method Close") {
		t.Fatalf("use handle acquired scope shutdown authority: %v\n%s", err, output)
	}
}
