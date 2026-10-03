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

// Package configuration supplies complete typed configuration scenarios through
// public Adapters. Projects declare their schema, captured values and an explicit
// immutable Provider. They do not construct Adapter runtimes, clients or inboxes.
//
// ReadInputs binds supplied arguments, an explicit environment lookup, selected
// dotenv and project defaults. It performs no ambient lookup, provider inference
// or process mutation. Viper and Nacos associate every original document with its
// layer policy. PrepareNacos loads the supported deployment bootstrap through
// the same typed Load path, returning an inert provider and released facts.
//
// Load owns finite acquisition, strict preparation, publication and cleanup.
// Result separates accepted State from released records and cleanup failure.
// Watch owns a source and serial validation until actual release. Invalid and
// obsolete updates preserve last-good data; timed-out Close retains ownership.
// No second native parser, poller or resource engine is introduced.
//
// Settings types, defaults, business validation, permitted input names and
// environment-to-document choices remain project-owned. Optional Resources is
// borrowed for adoption, never closed here. Acceptance is not instance readiness.
// Independent declarations retain independent stores, sources, limits and records;
// framework.Runtime does not implicitly become their parent or shared quota.
//
// Records transfer in-process facts to the caller, not a durable audit sink.
// Components supplies shared and supported-provider localization definitions
// without opening clients. Source causes and explicit data copies are sensitive.
// This package neither installs settings.Default nor starts a Worker/application
// host. Process-local loading and Watch must not run inside deterministic Workflows.
package configuration
