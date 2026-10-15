/*
fathomry
Copyright (C) 2026  Frost Leo
SPDX-License-Identifier: GPL-3.0-or-later

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>.
*/

// Package mail exposes managed MIME preparation, SMTP and explicit local delivery
// over the selected go-mail integration. Prepare is offline; PrepareMessage is an
// admitted operation. Open returns an owning source; Client and Handle cannot close
// it. Using retains a Fixed/Follow generation for each operation. Results and the
// independent Inbox preserve effects without retrying notification mutations.
package mail
