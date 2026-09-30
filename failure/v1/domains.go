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

package failure

// Domain classifies a capability, independently of Adapter/Framework/CLI layers
// and SDK brands. Empty means invalid or not assigned in the current range map.
type Domain string

// Established capability labels do not encode a code layer or a provider brand.
// Application is for project/business semantics; it is not a Framework catch-all.
const (
	DomainCore          Domain = "core"
	DomainConfiguration Domain = "configuration"
	DomainDatabase      Domain = "database"
	DomainCache         Domain = "cache"
	DomainObjectStorage Domain = "object_storage"
	DomainMessaging     Domain = "messaging"
	DomainOrchestration Domain = "orchestration"
	DomainNetwork       Domain = "network"
	DomainObservability Domain = "observability"
	DomainNotification  Domain = "notification"
	DomainBrowser       Domain = "browser"
	DomainTableFormat   Domain = "table_format"
	DomainApplication   Domain = "application"
)

// DomainRange assigns an inclusive base facility band to one capability. First
// and Last are below FirstExtensionFacility. Extensions use the same band plus
// FirstExtensionFacility; its slots do not identify the same first-party owners.
// The core base starts at zero, but facility zero itself remains invalid.
//
// Bands are allocation policy, not additional fixed-width Code fields. A domain
// may acquire another non-overlapping band without renumbering existing codes.
type DomainRange struct {
	Domain Domain   `json:"domain"`
	First  Facility `json:"first"`
	Last   Facility `json:"last"`
}

// Domains returns the detached capability-range manifest, in base-facility order.
// Unlisted bands remain reserved; declaration admission rejects them. This table
// classifies numbers, not available SDKs or a promise of public Adapter coverage.
//
// This is the accepted capability-first policy, not a provisional package layout.
// Database includes ordinary databases and SQL engines; observability includes
// logging, tracing and metrics. Classify what the public capability does, not its
// backend brand: Redis caching and Redis-backed configuration belong to different
// domains, and Iceberg table semantics are not object storage merely because of S3.
// An Adapter and a Framework scenario for the same capability use the same domain.
// Forwarding an existing error alone never justifies a new code or owner.
//
// Each row describes the first-party base band; the extension mirror adds 0x400
// to both endpoints (database 0x080..0x0FF mirrors to 0x480..0x4FF). Facility zero
// remains invalid even though the core base starts at zero. Application means
// project/business semantics, never "everything implemented by Framework".
//
// 0x380..0x3FF and its 0x780..0x7FF mirror are reserved growth, not free-form slots.
// Future capabilities may add reviewed, non-overlapping bands there. Existing
// assignments must not move, and these ranges must not become new fixed-width
// domain/component subfields inside Code. Keep the manifests, tests and documented
// allocation policy aligned when extending coverage.
func Domains() []DomainRange {
	return []DomainRange{
		{Domain: DomainCore, First: 0x000, Last: 0x03F},
		{Domain: DomainConfiguration, First: 0x040, Last: 0x07F},
		{Domain: DomainDatabase, First: 0x080, Last: 0x0FF},
		{Domain: DomainCache, First: 0x100, Last: 0x13F},
		{Domain: DomainObjectStorage, First: 0x140, Last: 0x17F},
		{Domain: DomainMessaging, First: 0x180, Last: 0x1BF},
		{Domain: DomainOrchestration, First: 0x1C0, Last: 0x1FF},
		{Domain: DomainNetwork, First: 0x200, Last: 0x27F},
		{Domain: DomainObservability, First: 0x280, Last: 0x2BF},
		{Domain: DomainNotification, First: 0x2C0, Last: 0x2FF},
		{Domain: DomainBrowser, First: 0x300, Last: 0x31F},
		{Domain: DomainTableFormat, First: 0x320, Last: 0x33F},
		{Domain: DomainApplication, First: 0x340, Last: 0x37F},
	}
}

// Domain identifies this facility's capability band. Invalid facilities and
// reserved growth ranges return empty. Extension and first-party facilities in
// corresponding bands have the same capability, not the same allocation owner.
func (facility Facility) Domain() Domain {
	if !facility.Valid() {
		return ""
	}
	base := facility
	if base.Extension() {
		base -= FirstExtensionFacility
	}
	for _, band := range Domains() {
		if base >= band.First && base <= band.Last {
			return band.Domain
		}
	}
	return ""
}

// Domain classifies a well-formed code through the facility-range manifest. Empty
// means an invalid code or an as-yet-unassigned domain; Code.Valid distinguishes
// those cases. Classification does not imply that this error is in a catalog.
func (code Code) Domain() Domain { return code.Facility().Domain() }
