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
	"github.com/frost-leo/fathomry/adapters/httpclient/httpcloak/v1"
	"github.com/frost-leo/fathomry/adapters/httpclient/nethttp/v1"
	"github.com/frost-leo/fathomry/adapters/httpclient/nuki/v1"
	"github.com/frost-leo/fathomry/adapters/httpclient/surf/v1"
	"github.com/frost-leo/fathomry/adapters/httpclient/tlsclient/v1"
	logging "github.com/frost-leo/fathomry/adapters/logging/v1"
	zap "github.com/frost-leo/fathomry/adapters/logging/zap/v1"
	"github.com/frost-leo/fathomry/adapters/objectstore/minio/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/doris/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/duckdb/v1"
	"github.com/frost-leo/fathomry/adapters/sqlengine/trino/v1"
	"github.com/frost-leo/fathomry/adapters/telemetry/otel/v1"
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
		i18n.Component{Module: "fathomry", Name: "database_duckdb", BaseLocale: "en", Resources: duckdb.Resources(), Directory: "resources", Definitions: duckdb.Definitions()},
		i18n.Component{Module: "fathomry", Name: "database_trino", BaseLocale: "en", Resources: trino.Resources(), Directory: "resources", Definitions: trino.Definitions()},
		i18n.Component{Module: "fathomry", Name: "database_doris", BaseLocale: "en", Resources: doris.Resources(), Directory: "resources", Definitions: doris.Definitions()},
		i18n.Component{Module: "fathomry", Name: "objectstore_minio", BaseLocale: "en", Resources: minio.Resources(), Directory: "resources", Definitions: minio.Definitions()},
		i18n.Component{Module: "fathomry", Name: "http_nethttp", BaseLocale: "en", Resources: nethttp.Resources(), Directory: "resources", Definitions: nethttp.Definitions()},
		i18n.Component{Module: "fathomry", Name: "http_tlsclient", BaseLocale: "en", Resources: tlsclient.Resources(), Directory: "resources", Definitions: tlsclient.Definitions()},
		i18n.Component{Module: "fathomry", Name: "http_surf", BaseLocale: "en", Resources: surf.Resources(), Directory: "resources", Definitions: surf.Definitions()},
		i18n.Component{Module: "fathomry", Name: "http_httpcloak", BaseLocale: "en", Resources: httpcloak.Resources(), Directory: "resources", Definitions: httpcloak.Definitions()},
		i18n.Component{Module: "fathomry", Name: "http_nuki", BaseLocale: "en", Resources: nuki.Resources(), Directory: "resources", Definitions: nuki.Definitions()},
		i18n.Component{Module: "fathomry", Name: "telemetry_otel", BaseLocale: "en", Resources: otel.Resources(), Directory: "resources", Definitions: otel.Definitions()},
		i18n.Component{Module: "fathomry", Name: "logging", BaseLocale: "en", Resources: logging.Resources(), Directory: "resources", Definitions: logging.Definitions()},
		i18n.Component{Module: "fathomry", Name: "logging_zap", BaseLocale: "en", Resources: zap.Resources(), Directory: "resources", Definitions: zap.Definitions()},
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
