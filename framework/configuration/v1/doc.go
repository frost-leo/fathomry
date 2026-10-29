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

// Package configuration loads explicit bootstrap inputs and publishes validated
// project-owned settings through finite local/Viper or remote/Nacos sessions.
//
// LoadBootstrap borrows separate local dependencies. Start admits and copies its
// inputs before launching work; WaitInitial observes the first outcome without
// owning the session lifetime. Reader exposes accepted data, while Next and Status
// report bounded source, validation and cleanup facts. Close fences publication
// and joins owned work; retain incomplete owners and continue Close.
//
// Projects supply a non-nil bounded, pure, non-panicking validator. Its closure
// is borrowed behavior, not copied data. Providers own acquisition and recovery;
// required evidence remains in caller-owned Inboxes until explicit reception/Ack.
// No process defaults, environment scans, source writes, manual refresh, resource
// reconstruction, application host or deterministic Workflow execution is provided.
package configuration
