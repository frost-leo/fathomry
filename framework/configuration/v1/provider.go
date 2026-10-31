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

	nacos "github.com/frost-leo/fathomry/adapters/configsource/nacos/v1"
	configsource "github.com/frost-leo/fathomry/adapters/configsource/v1"
	viper "github.com/frost-leo/fathomry/adapters/configsource/viper/v1"
	"github.com/frost-leo/fathomry/adapters/v1"
)

func budget[T any](runtime *adapters.Runtime, inbox *adapters.Inbox[T], observer *adapters.Observer, roots int, work int64) error {
	options, err := runtime.Options()
	if err != nil {
		return at(ErrDeclaration, "runtime", -1, err)
	}
	evidence, err := inbox.Options()
	if err != nil {
		return at(ErrDeclaration, "evidence", -1, err)
	}
	if observer != nil {
		if _, err := observer.Inspect(); err != nil {
			return at(ErrDeclaration, "observer", -1, err)
		}
	}
	if options.MaxActive < roots || options.MaxWorkBytes < work || evidence.Capacity < roots || evidence.MaxBytes < int64(roots)*(64<<10) {
		return at(ErrLimit, "capacity", -1)
	}
	return nil
}
func localBudget(dependencies viper.Dependencies, watch bool) error {
	work := int64(8 << 20)
	if watch {
		work = 24 << 20
	}
	return budget(dependencies.Runtime, dependencies.Evidence, dependencies.Observer, 1, work)
}
func providerBudget(value SourcePlan, dependencies Dependencies) error {
	if value.Local != nil {
		return localBudget(dependencies.Local, value.Watch)
	}
	work := int64(17 << 20)
	if value.Watch {
		capacity := value.Remote.Observation.QueueCapacity
		if capacity == 0 {
			capacity = 2
		}
		work += int64(capacity) * (4 << 20)
	}
	return budget(dependencies.Remote.Runtime, dependencies.Remote.Evidence, dependencies.Remote.Observer, 2, work)
}

func openSource(ctx context.Context, value SourcePlan, dependencies Dependencies) (configsource.Source, *nacos.Owner, error) {
	if value.Local != nil {
		client, err := viper.New(dependencies.Local)
		if err != nil {
			return nil, nil, err
		}
		source, err := client.Source(*value.Local)
		return source, nil, err
	}
	owner, err := nacos.Open(ctx, value.Remote.Settings, dependencies.Remote)
	if err != nil || owner == nil {
		return nil, owner, err
	}
	source, err := owner.Client().Source(value.Remote.Observation)
	return source, owner, err
}
