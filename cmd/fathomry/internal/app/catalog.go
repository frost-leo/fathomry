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

package app

import (
	"github.com/frost-leo/fathomry/adapters/broker/kafka/v1"
	"github.com/frost-leo/fathomry/adapters/cache/redis/v1"
	"github.com/frost-leo/fathomry/adapters/database/mysql/v1"
	"github.com/frost-leo/fathomry/adapters/database/postgres/v1"
	"github.com/frost-leo/fathomry/adapters/objectstore/minio/v1"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/errorcatalog"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/messages"
	"github.com/frost-leo/fathomry/cmd/fathomry/internal/command/project"
	"github.com/frost-leo/fathomry/failure/v1"
	configuration "github.com/frost-leo/fathomry/framework/configuration/v1"
	"github.com/frost-leo/fathomry/i18n/v1"
)

// catalogs is offline metadata composition, not Framework runtime construction.
func catalogs() (command.Catalogs, error) {
	components := append(configuration.Components(),
		command.Component(),
		errorcatalog.Component(),
		messages.Component(),
		project.Component(),
		i18n.Component{Module: "fathomry", Name: "cache_redis", BaseLocale: "en", Resources: redis.CacheResources(), Directory: ".", Definitions: redis.CacheDefinitions()},
		i18n.Component{Module: "fathomry", Name: "messaging_redis", BaseLocale: "en", Resources: redis.MessagingResources(), Directory: ".", Definitions: redis.MessagingDefinitions()},
		i18n.Component{Module: "fathomry", Name: "broker_kafka", BaseLocale: "en", Resources: kafka.Resources(), Directory: "resources", Definitions: kafka.Definitions()},
		i18n.Component{Module: "fathomry", Name: "database_postgres", BaseLocale: "en", Resources: postgres.Resources(), Directory: "resources", Definitions: postgres.Definitions()},
		i18n.Component{Module: "fathomry", Name: "database_mysql", BaseLocale: "en", Resources: mysql.Resources(), Directory: "resources", Definitions: mysql.Definitions()},
		i18n.Component{Module: "fathomry", Name: "objectstore_minio", BaseLocale: "en", Resources: minio.Resources(), Directory: "resources", Definitions: minio.Definitions()},
	)
	var definitions []failure.Definition
	for _, component := range components {
		definitions = append(definitions, component.Definitions...)
	}
	errors, err := failure.Prepare(definitions...)
	if err != nil {
		return command.Catalogs{}, err
	}
	translations, err := i18n.Prepare(components...)
	if err != nil {
		return command.Catalogs{}, err
	}
	return command.Catalogs{Errors: errors, Messages: translations}, nil
}
