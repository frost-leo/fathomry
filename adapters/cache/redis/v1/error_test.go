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

package redis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frost-leo/fathomry/adapters/v1"
	"github.com/frost-leo/fathomry/failure/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
	native "github.com/frost-leo/fathomry/internal/cache/redis/v9"
	"github.com/frost-leo/fathomry/internal/fault"
	"github.com/frost-leo/fathomry/internal/invocation"
	sdk "github.com/redis/go-redis/v9"
)

func TestCapabilityErrorsLocalePrivacyAndForwarding(t *testing.T) {
	catalog, err := failure.Prepare(Definitions()...)
	if err != nil || catalog == nil {
		t.Fatal(err)
	}
	translations, err := i18n.Prepare(
		i18n.Component{Module: "fathomry", Name: "cache_redis", BaseLocale: "en", Resources: CacheResources(), Directory: ".", Definitions: CacheDefinitions()},
		i18n.Component{Module: "fathomry", Name: "messaging_redis", BaseLocale: "en", Resources: MessagingResources(), Directory: ".", Definitions: MessagingDefinitions()},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, definition := range Definitions() {
		expected := failure.DomainCache
		if strings.HasPrefix(string(definition.Identifier), "fathomry.messaging_redis.") {
			expected = failure.DomainMessaging
		}
		if definition.Code.Facility().Domain() != expected {
			t.Fatal("capability domain mismatch")
		}
		for _, locale := range []string{"en", "zh-CN", "zh-Hans-CN", "fr"} {
			_, found, err := translations.Explain(definition.Code, locale)
			if err != nil || !found {
				t.Fatal("offline locale gap", err)
			}
		}
	}
	primary := native.ErrCommand.New(fault.Context{}, sdk.Nil)
	cleanup := native.ErrCleanup.New(fault.Context{}, context.Canceled)
	wrapped := translate(errors.Join(primary, cleanup), "synthetic", Messaging)
	if !errors.Is(wrapped, ErrMessagingCommand) || !errors.Is(wrapped, ErrMessagingCleanup) || !errors.Is(wrapped, sdk.Nil) || !errors.Is(wrapped, context.Canceled) {
		t.Fatal("joined causes lost")
	}
	original, _ := failure.New(adapters.Definitions()[0], failure.Location{Operation: "synthetic"})
	forwarded := translate(invocation.ErrFailed.New(fault.Context{}, original), "callback", Messaging)
	if forwarded != original {
		t.Fatal("shared occurrence identity changed")
	}
	canary := "private-payload-canary"
	command := command(t, Messaging, "EVAL", canary, "0")
	for _, value := range []any{command, Settings{Password: canary, Addrs: []string{canary}}, problem(Messaging, ErrCommand, "operation", errors.New(canary)),
		&Owner{}, &Client{}, &Handle{}, Result{}, Reply{}, &Password{}, &Session{}, &Subscription{}, SubscriptionOptions{Channels: []string{canary}}} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", value, value, value), canary) {
			t.Fatal("formatting leaked")
		}
		var output bytes.Buffer
		slog.New(slog.NewJSONHandler(&output, nil)).Info("safe", "value", value)
		if strings.Contains(output.String(), canary) {
			t.Fatal("slog leaked")
		}
	}
	for _, value := range []any{command, &Owner{}, &Client{}, Result{}, Reply{}, Value{}, &Session{}, &Subscription{}, Prepared{}} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("runtime serialization accepted")
		}
	}
}

func TestWrappedSharedErrorPrivacyAndOriginalGraph(t *testing.T) {
	canary := "private-callback-prefix"
	cause := errors.New("private-underlying-cause")
	original, err := failure.New(adapters.Definitions()[0], failure.Location{Operation: "synthetic"}, cause)
	if err != nil {
		t.Fatal(err)
	}
	wrapped := fmt.Errorf("%s: %w", canary, original)
	got := translate(invocation.ErrFailed.New(fault.Context{}, wrapped), "callback", Messaging)
	core, ok := failure.Inspect(got)
	if !ok || core != original || !errors.Is(got, wrapped) || !errors.Is(got, original) || !errors.Is(got, cause) {
		t.Fatal("forwarded identity/graph changed")
	}
	var exact *failure.Error
	if !errors.As(got, &exact) || exact != original {
		t.Fatal("original core inspection lost")
	}
	for _, value := range []error{got, translate(fmt.Errorf("%s: %w", canary, errors.Join(original, original)), "callback", Messaging)} {
		if strings.Contains(fmt.Sprintf("%v %+v %#v %s %q", value, value, value, value, value), canary) || strings.Contains(value.Error(), canary) {
			t.Fatal("callback text leaked")
		}
		var out bytes.Buffer
		slog.New(slog.NewJSONHandler(&out, nil)).Info("safe", "error", value)
		if strings.Contains(out.String(), canary) {
			t.Fatal("slog callback text leaked")
		}
	}
}

type spyLogger struct{ count atomic.Int32 }

func (logger *spyLogger) Printf(context.Context, string, ...interface{}) { logger.count.Add(1) }
func TestNativeLoggingBootstrapSubprocess(t *testing.T) {
	mode := os.Getenv("FATHOMRY_REDIS_LOG_TEST")
	if mode == "" {
		for _, mode := range []string{"keep", "disable"} {
			t.Run(mode, func(t *testing.T) {
				child := exec.CommandContext(testContext(t), os.Args[0], "-test.run=^TestNativeLoggingBootstrapSubprocess$")
				child.Env = append(os.Environ(), "FATHOMRY_REDIS_LOG_TEST="+mode)
				if output, err := child.CombinedOutput(); err != nil {
					t.Fatalf("logging subprocess: %v\n%s", err, output)
				}
			})
		}
		return
	}
	logger := &spyLogger{}
	sdk.SetLogger(logger)
	if mode == "disable" {
		DisableNativeLogging()
	}
	for range 2 {
		owner, _ := openTest(t, testSettings("127.0.0.1:1"))
		_ = owner.Close(testContext(t))
		_ = Definitions()
		_ = CacheResources()
		_ = MessagingResources()
	}
	// The pinned pool reports exhausted dial attempts through its global logger.
	probe := sdk.NewClient(&sdk.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialerRetries: 1, DialTimeout: 20 * time.Millisecond, DisableIdentity: true})
	defer probe.Close()
	_ = probe.Ping(testContext(t)).Err()
	if mode == "keep" && logger.count.Load() == 0 {
		t.Fatal("construction silently replaced global logger")
	}
	if mode == "disable" && logger.count.Load() != 0 {
		t.Fatal("explicit bootstrap did not suppress native logging")
	}
}
