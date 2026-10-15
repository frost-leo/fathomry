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

import React from 'react';
import { createRoot } from 'react-dom/client';
import { flushSync } from 'react-dom';
import { ActivityCalendar } from 'react-activity-calendar';

// Only invoked by the isolated screenshot fixture, never by the SMTP Provider.
window.renderFathomryCalendar = (node, snapshot) => {
  const applicability = new Map(snapshot.calendar.map(day => [day.day, day.applicable]));
  const data = snapshot.calendar.map(day => ({
    date: day.day, count: day.count,
    level: day.count === 0 ? 0 : day.count === 1 ? 1 : day.count <= 3 ? 2 : day.count <= 7 ? 3 : 4
  }));
  const root = createRoot(node);
  flushSync(() => root.render(React.createElement(ActivityCalendar, {
    data, blockSize: 9, blockMargin: 3, blockRadius: 2, fontSize: 12,
    colorScheme: 'light', weekStart: 0, showWeekdayLabels: ['mon', 'wed', 'fri'],
    labels: { totalCount: '{{count}} commits in the selected window', legend: { less: 'Less', more: 'More' } },
    theme: { light: ['#EFF1F5', '#B5E8B8', '#75D184', '#36AE57', '#19833C'] },
    style: { color: '#63636D', fontFamily: 'Arial, sans-serif' },
    renderBlock: (block, activity) => applicability.get(activity.date) ? block : React.cloneElement(block, {
      fill: '#FFFFFF', stroke: '#EBECF0', strokeWidth: 1,
      'aria-label': activity.date + ': before repository creation'
    })
  })));
  return () => flushSync(() => root.unmount());
};
