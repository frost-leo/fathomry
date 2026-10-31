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

package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	cli "github.com/frost-leo/fathomry/cmd/fathomry/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type applicationConfig struct {
	Name string           `json:"name"`
	I18n i18n.Preferences `json:"i18n"`
}
type projectConfig struct {
	Application applicationConfig `json:"application"`
}

var validationRefused = errors.New("private-project-validator")

func schemaForProject() configuration.Schema[projectConfig] {
	return configuration.Schema[projectConfig]{Version: 1, Validate: func(ctx context.Context, value projectConfig) error {
		if value.Application.Name != "example" {
			return validationRefused
		}
		return errors.Join(value.Application.I18n.Validate(), ctx.Err())
	}}
}
func projectSchema(schema configuration.Schema[projectConfig]) configuration.SchemaFactory[projectConfig] {
	return func(context.Context, string) (configuration.Schema[projectConfig], error) { return schema, nil }
}

func projectFile(t *testing.T, path, text string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
}
func projectFiles(t *testing.T, watch bool) string {
	t.Helper()
	directory := t.TempDir()
	flag := "false"
	if watch {
		flag = "true"
	}
	projectFile(t, filepath.Join(directory, ".env"), "FATHOMRY_ENV=development\nFATHOMRY_CONFIG_MODE=local\nFATHOMRY_CONFIG_PROVIDER=viper\nFATHOMRY_CONFIG_WATCH="+flag+"\n")
	projectFile(t, filepath.Join(directory, "config/base.yaml"), "application:\n  name: example\n  i18n:\n    locale: en\n")
	projectFile(t, filepath.Join(directory, "config/environment/development.yaml"), "application:\n  i18n:\n    locale: zh-CN\n")
	projectFile(t, filepath.Join(directory, "config/environment/production.yaml"), "{}")
	return directory
}

type projectReport struct {
	Data struct {
		Accepted          bool   `json:"accepted"`
		Environment       string `json:"environment"`
		Application       string `json:"application"`
		ApplicationLocale string `json:"application_locale"`
		OutputLocale      string `json:"output_locale"`
		Watching          bool   `json:"watching"`
	} `json:"data"`
}

func TestProjectConfigurationCommand(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(map[bool]string{false: "finite", true: "watch"}[watch], func(t *testing.T) {
			directory := projectFiles(t, watch)
			var output, diagnostics bytes.Buffer
			var evidence atomic.Int32
			err := cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env"), "--output=json"},
				cli.Project[projectConfig]{Schema: projectSchema(schemaForProject()), Configuration: configuration.Policy{Receive: func(_ context.Context, value configuration.Evidence) error {
					if value.Viper == nil || !value.Viper.Info().Released {
						t.Error("unreleased evidence")
					}
					evidence.Add(1)
					return nil
				}}},
				cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
			if err != nil {
				t.Fatal(err, diagnostics.String())
			}
			var report projectReport
			if json.Unmarshal(output.Bytes(), &report) != nil || !report.Data.Accepted || report.Data.Application != "example" ||
				report.Data.ApplicationLocale != "zh-CN" || report.Data.OutputLocale != "zh-CN" || report.Data.Watching != watch {
				t.Fatal("invalid selected report", output.String())
			}
			if evidence.Load() != 2 || settings.Configured() {
				t.Fatal("cleanup/evidence or default contract violated")
			}
			if strings.Contains(output.String()+diagnostics.String(), directory) {
				t.Fatal("private path exposed")
			}
		})
	}
}
func TestProjectHelpHasNoBootstrapEffects(t *testing.T) {
	for _, args := range [][]string{nil, {"--help"}, {"help", "config"}, {"config"}, {"config", "check", "--help"}, {"config", "check", "--unknown"}} {
		var output, diagnostics bytes.Buffer
		calls := 0
		err := cli.RunProject(context.Background(), args, cli.Project[projectConfig]{
			Schema: func(context.Context, string) (configuration.Schema[projectConfig], error) {
				calls++
				return schemaForProject(), nil
			},
			LookupEnv: func(string) (string, bool) { calls++; return "invalid", true }},
			cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
		if calls != 0 {
			t.Fatal("offline help captured bootstrap environment")
		}
		invalid := len(args) > 0 && args[len(args)-1] == "--unknown"
		if invalid && cli.ExitCode(err) != 2 || !invalid && err != nil {
			t.Fatal(err, diagnostics.String())
		}
	}
}
func TestProjectSelectionAndLocaleOverride(t *testing.T) {
	directory := projectFiles(t, false)
	for _, explicit := range []bool{false, true} {
		var output, diagnostics bytes.Buffer
		calls := map[string]int{}
		args := []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env"), "--output=json"}
		if explicit {
			args = append(args, "--env=development", "--lang=en")
		}
		err := cli.RunProject(context.Background(), args, cli.Project[projectConfig]{Schema: projectSchema(schemaForProject()), LookupEnv: func(key string) (string, bool) {
			calls[key]++
			if key == "FATHOMRY_ENV" {
				return "production", true
			}
			return "", false
		}}, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
		if err != nil {
			t.Fatal(err, diagnostics.String())
		}
		if len(calls) != len(configuration.BootstrapKeys()) {
			t.Fatal("wrong environment capture set")
		}
		for _, count := range calls {
			if count != 1 {
				t.Fatal("variable reread")
			}
		}
		var report projectReport
		if json.Unmarshal(output.Bytes(), &report) != nil {
			t.Fatal("invalid output")
		}
		if explicit && report.Data.Environment != "development" || !explicit && report.Data.Environment != "production" || report.Data.OutputLocale != "en" {
			t.Fatal("override lost", output.String())
		}
	}
}
func TestProjectValidationAndMissingLayers(t *testing.T) {
	directory := projectFiles(t, false)
	var output, diagnostics bytes.Buffer
	declared := schemaForProject()
	declared.Validate = func(context.Context, projectConfig) error { return validationRefused }
	err := cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env")}, cli.Project[projectConfig]{Schema: projectSchema(declared)},
		cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
	var retained *cli.ExecutionError
	if !errors.Is(err, validationRefused) || !errors.As(err, &retained) || !retained.CleanupComplete() || output.Len() != 0 {
		t.Fatal("validation failure/ownership lost", err)
	}
	if strings.Contains(diagnostics.String(), validationRefused.Error()) {
		t.Fatal("validator detail exposed")
	}
	if err := os.Remove(filepath.Join(directory, "config/environment/development.yaml")); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	diagnostics.Reset()
	err = cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env")}, cli.Project[projectConfig]{Schema: projectSchema(schemaForProject())},
		cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
	if !errors.Is(err, configuration.ErrMissing) || output.Len() != 0 {
		t.Fatal("missing layer fallback", err)
	}
}

type brokenOutput struct{ err error }

func (output brokenOutput) Write([]byte) (int, error) { return 0, output.err }
func TestProjectOutputErrorAfterCleanup(t *testing.T) {
	directory := projectFiles(t, false)
	var received atomic.Int32
	problem := errors.New("private-output-error")
	var diagnostics bytes.Buffer
	err := cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env")}, cli.Project[projectConfig]{
		Schema: projectSchema(schemaForProject()), Configuration: configuration.Policy{Receive: func(context.Context, configuration.Evidence) error { received.Add(1); return nil }}},
		cli.Options{Input: strings.NewReader(""), Output: brokenOutput{problem}, ErrorOutput: &diagnostics})
	var retained *cli.ExecutionError
	if !errors.Is(err, problem) || !errors.As(err, &retained) || !retained.CleanupComplete() || received.Load() != 2 {
		t.Fatal("output error lost actual cleanup", err)
	}
	if strings.Contains(diagnostics.String(), problem.Error()) {
		t.Fatal("output cause exposed")
	}
	check := retainedConfiguration(t, err)
	assertAcceptedConfiguration(t, check)
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Info("check", "error", check)
	if strings.Contains(fmt.Sprintf("%#v %+v %v", check, check, check)+log.String(), problem.Error()) {
		t.Fatal("retained configuration error exposed a native cause")
	}
	if _, err := json.Marshal(check); err == nil {
		t.Fatal("configuration result serialized")
	}
	if err := json.Unmarshal([]byte("{}"), check); err == nil {
		t.Fatal("configuration result reconstructed")
	}
}
func TestProjectEvidenceFailureKeepsScope(t *testing.T) {
	directory := projectFiles(t, false)
	var reject atomic.Bool
	reject.Store(true)
	problem := errors.New("private-evidence-receiver")
	var output, diagnostics bytes.Buffer
	err := cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env")}, cli.Project[projectConfig]{
		Schema: projectSchema(schemaForProject()), Configuration: configuration.Policy{Receive: func(context.Context, configuration.Evidence) error {
			if reject.Load() {
				return problem
			}
			return nil
		}}}, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
	var retained *cli.ExecutionError
	if !errors.Is(err, problem) || !errors.As(err, &retained) || retained.CleanupComplete() || output.Len() != 0 {
		t.Fatal("pending custody lost", err)
	}
	var check *cli.ConfigurationCheckError
	if errors.As(err, &check) {
		t.Fatal("bootstrap failure retained accepted-looking configuration")
	}
	reject.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = retained.Close(ctx)
	if !retained.CleanupComplete() {
		t.Fatal("existing cleanup continuation failed")
	}
}

func retainedConfiguration(t *testing.T, err error) *cli.ConfigurationCheckError {
	t.Helper()
	var check *cli.ConfigurationCheckError
	var execution *cli.ExecutionError
	if !errors.As(err, &check) || !errors.As(err, &execution) {
		t.Fatal("accepted configuration or existing cleanup owner lost", err)
	}
	core, present := failure.Inspect(err)
	if !present || core != execution.Failure() {
		t.Fatal("result retention changed the original error occurrence")
	}
	return check
}

func assertAcceptedConfiguration(t *testing.T, check *cli.ConfigurationCheckError) {
	t.Helper()
	view, err := check.Configuration().InitialView()
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := settings.As[projectConfig](view)
	if err != nil {
		t.Fatal(err)
	}
	value, err := snapshot.ValueCopy()
	if err != nil || value.Application.Name != "example" || value.Application.I18n.Locale != "zh-CN" {
		t.Fatal("retained initial configuration changed", err)
	}
}

func TestProjectPostAcceptanceFailureRetainsConfiguration(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(fmt.Sprint(watch), func(t *testing.T) {
			directory := projectFiles(t, watch)
			var received atomic.Int32
			var reject atomic.Bool
			reject.Store(true)
			problem := errors.New("required-configuration-evidence")
			var output, diagnostics bytes.Buffer
			err := cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env"), "--output=json"},
				cli.Project[projectConfig]{Schema: projectSchema(schemaForProject()), Configuration: configuration.Policy{
					Receive: func(context.Context, configuration.Evidence) error {
						if received.Add(1) > 1 && reject.Load() {
							return problem
						}
						return nil
					},
				}}, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
			if !errors.Is(err, problem) || output.Len() != 0 {
				t.Fatal("required evidence failure hidden", err)
			}
			check := retainedConfiguration(t, err)
			assertAcceptedConfiguration(t, check)
			var execution *cli.ExecutionError
			if !errors.As(err, &execution) || execution.CleanupComplete() {
				t.Fatal("unfinished custody discarded")
			}
			projectFile(t, filepath.Join(directory, "config/base.yaml"), "application:\n  name: changed-after-acceptance\n")
			reject.Store(false)
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = execution.Close(cleanup)
			if !execution.CleanupComplete() || received.Load() != 3 {
				t.Fatal("continued cleanup reread sources or lost custody")
			}
			assertAcceptedConfiguration(t, check)
			if strings.Contains(diagnostics.String(), problem.Error()) || strings.Contains(diagnostics.String(), directory) {
				t.Fatal("safe diagnostic exposed private details")
			}
		})
	}
}

type postAcceptanceContext struct {
	context.Context
	cancel        context.CancelFunc
	cancelOnLease bool
	reachedLease  atomic.Bool
}

func (ctx *postAcceptanceContext) Err() error {
	if !ctx.reachedLease.Load() {
		var callers [16]uintptr
		frames := runtime.CallersFrames(callers[:runtime.Callers(2, callers[:])])
		for {
			frame, more := frames.Next()
			if strings.Contains(frame.Function, "github.com/frost-leo/fathomry/resource/v1.(*binding[") && strings.HasSuffix(frame.Function, ").acquire") {
				if ctx.reachedLease.CompareAndSwap(false, true) && ctx.cancelOnLease {
					ctx.cancel()
				}
				break
			}
			if !more {
				break
			}
		}
	}
	return ctx.Context.Err()
}

func TestProjectCancellationAfterAdoptionRetainsConfiguration(t *testing.T) {
	for _, watch := range []bool{false, true} {
		for _, cancelOnLease := range []bool{false, true} {
			t.Run(fmt.Sprintf("watch=%t/cancel=%t", watch, cancelOnLease), func(t *testing.T) {
				directory := projectFiles(t, watch)
				base, cancel := context.WithCancel(context.Background())
				defer cancel()
				// Reaching lease admission proves construction and adoption succeeded.
				ctx := &postAcceptanceContext{Context: base, cancel: cancel, cancelOnLease: cancelOnLease}
				var output, diagnostics bytes.Buffer
				var received atomic.Int32
				err := cli.RunProject(ctx, []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env"), "--output=json"},
					cli.Project[projectConfig]{Schema: projectSchema(schemaForProject()), Configuration: configuration.Policy{
						Receive: func(context.Context, configuration.Evidence) error { received.Add(1); return nil },
					}}, cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
				if !ctx.reachedLease.Load() || received.Load() != 2 {
					t.Fatal("post-adoption control or source cleanup not reached", err)
				}
				if !cancelOnLease {
					if err != nil || output.Len() == 0 {
						t.Fatal("noncanceling control failed", err)
					}
					return
				}
				if !errors.Is(err, context.Canceled) || output.Len() != 0 {
					t.Fatal("cancellation was suppressed or emitted success", err)
				}
				check := retainedConfiguration(t, err)
				assertAcceptedConfiguration(t, check)
				var execution *cli.ExecutionError
				if !errors.As(err, &execution) || !execution.CleanupComplete() {
					t.Fatal("original cleanup ownership lost", err)
				}
				if strings.Contains(diagnostics.String(), directory) {
					t.Fatal("cancellation diagnostic exposed private paths")
				}
			})
		}
	}
}

func TestProjectCancellationBeforeAcceptanceHasNoConfiguration(t *testing.T) {
	for _, watch := range []bool{false, true} {
		t.Run(fmt.Sprint(watch), func(t *testing.T) {
			directory := projectFiles(t, watch)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			declared := schemaForProject()
			declared.Validate = func(candidate context.Context, _ projectConfig) error {
				cancel()
				<-candidate.Done()
				return candidate.Err()
			}
			var output, diagnostics bytes.Buffer
			err := cli.RunProject(ctx, []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env"), "--output=json"},
				cli.Project[projectConfig]{Schema: projectSchema(declared)},
				cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
			var execution *cli.ExecutionError
			var check *cli.ConfigurationCheckError
			if !errors.Is(err, context.Canceled) || !errors.As(err, &execution) || !execution.CleanupComplete() ||
				errors.As(err, &check) || output.Len() != 0 {
				t.Fatal("pre-acceptance cancellation lost cleanup or fabricated configuration", err)
			}
		})
	}
}

func TestProjectTextUsesSelectedApplicationLocale(t *testing.T) {
	directory := projectFiles(t, false)
	var output, diagnostics bytes.Buffer
	err := cli.RunProject(context.Background(), []string{"config", "check", "--bootstrap", filepath.Join(directory, ".env")},
		cli.Project[projectConfig]{Schema: projectSchema(schemaForProject())},
		cli.Options{Input: strings.NewReader(""), Output: &output, ErrorOutput: &diagnostics})
	if err != nil || !strings.Contains(output.String(), "配置校验通过") {
		t.Fatal("application presentation not consumed", err, output.String())
	}
}
