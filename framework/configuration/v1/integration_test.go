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

package configuration

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/resource/v1"
	"github.com/frost-leo/fathomry/settings/v1"
)

type credentials struct {
	Account string `json:"account"`
	Access  string `json:"access"`
	Secret  string `json:"secret"`
}

func TestIndependentDomainsAndAdoption(t *testing.T) {
	deps, runtime, client := dependencies(t)
	directory := t.TempDir()
	applicationPath, businessPath := filepath.Join(directory, "application.yaml"), filepath.Join(directory, "credentials.yaml")
	write(t, applicationPath, "service: {port: 1}\n")
	write(t, businessPath, "account: fixed\naccess: incomplete\n")
	applicationSource, err := client.Source(viper.WatchSettings{Paths: []string{applicationPath}, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	businessSource, err := client.Source(viper.WatchSettings{Paths: []string{businessPath}, Interval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("component construction rejected")
	bind := func(name string, policy resource.Policy) resource.Ref[uint16] {
		ref, err := resource.Bind(runtime.Resources(), resource.Binding[uint16, uint16]{
			Name: name, Policy: policy,
			Select: func(view settings.View) (uint16, error) {
				value, present, err := settings.Read(view, "/service/port", func(value uint16) uint16 { return value })
				if err != nil {
					return 0, err
				}
				if !present {
					return 0, errors.New("missing selected port")
				}
				return value, nil
			},
			Clone: func(value uint16) uint16 { return value }, Equal: func(left, right uint16) bool { return left == right },
			Build: func(_ context.Context, value uint16) (*resource.Instance[uint16], error) {
				if value == 3 {
					return nil, failure
				}
				return &resource.Instance[uint16]{Value: value}, nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	fixed, following := bind("fixed", resource.Fixed), bind("following", resource.Follow)
	applicationDependencies := deps
	applicationDependencies.Resources = runtime.Resources()
	application, err := Watch(context.Background(), Declaration[model]{Schema: modelSchema(), Source: applicationSource, Layers: []Layer{{Kind: configsource.Base, Encoding: configsource.YAML}}}, applicationDependencies, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer application.Close(context.Background())
	business, err := Watch(context.Background(), Declaration[credentials]{
		Schema: configsource.Schema[credentials]{Version: 1, Validate: func(_ context.Context, value credentials) error {
			if value.Account != "fixed" || value.Access == "" || value.Secret != "secret-"+value.Access {
				return errors.New("invalid related credential bundle")
			}
			return nil
		}},
		Source: businessSource, Layers: []Layer{{Kind: configsource.Base, Encoding: configsource.YAML}},
	}, deps, WatchOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer business.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accepted := nextDecision(t, application, true)
	if accepted.Status.Adoption == nil || accepted.Status.AdoptionError != nil {
		t.Fatal("explicit adoption not coordinated")
	}
	if err := accepted.Status.Adoption.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if event := nextDecision(t, business, false); event.Err == nil {
		t.Fatal("initial invalid credentials accepted")
	}
	if _, err := business.Reader().Capture(); !errors.Is(err, settings.ErrUnconfigured) {
		t.Fatal("invalid initial business domain was ready")
	}
	before, _ := application.Capture()
	write(t, businessPath, "account: fixed\naccess: first\nsecret: secret-first\n")
	nextDecision(t, business, true)
	businessBefore, _ := business.Capture()
	write(t, businessPath, "account: fixed\naccess: second\nsecret: wrong\n")
	nextDecision(t, business, false)
	retained, _ := business.Capture()
	if retained.Description().Revision != businessBefore.Description().Revision {
		t.Fatal("invalid credentials replaced last-good")
	}
	write(t, businessPath, "account: fixed\naccess: second\nsecret: secret-second\n")
	nextDecision(t, business, true)
	after, _ := application.Capture()
	if after.Description().Revision != before.Description().Revision {
		t.Fatal("business variables republished application")
	}
	bundle, _ := business.Capture()
	value, _ := bundle.ValueCopy()
	if value.Access != "second" || value.Secret != "secret-second" || value.Account != "fixed" {
		t.Fatal("credential fields mixed")
	}
	old, err := following.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	write(t, applicationPath, "service: {port: 2}\n")
	updated := nextDecision(t, application, true)
	if err := updated.Status.Adoption.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	for _, entry := range []struct {
		ref      resource.Ref[uint16]
		expected uint16
	}{{fixed, 1}, {following, 2}} {
		lease, err := entry.ref.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got, err := lease.Value()
		if err != nil || got != entry.expected {
			t.Fatal("wrong per-binding policy")
		}
		if err := lease.Release(); err != nil {
			t.Fatal(err)
		}
	}
	oldValue, _ := old.Value()
	if oldValue != 1 {
		t.Fatal("borrowed generation retargeted")
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	write(t, applicationPath, "service: {port: 3}\n")
	failedAdoption := nextDecision(t, application, true)
	if err := failedAdoption.Status.Adoption.Wait(ctx); !errors.Is(err, failure) {
		t.Fatal("construction failure hidden", err)
	}
	latest, _ := application.Capture()
	latestValue, _ := latest.ValueCopy()
	if latestValue.Service.Port != 3 {
		t.Fatal("adoption failure rewrote accepted settings")
	}
	lease, err := following.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := lease.Value()
	if got != 2 {
		t.Fatal("failed adoption replaced usable instance")
	}
	_ = lease.Release()
}
