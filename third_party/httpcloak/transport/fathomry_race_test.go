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

package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFathomryOwnedTCPRaceBoundsAndJoinsLateLoser(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	loser, loserPeer := net.Pipe()
	winner, winnerPeer := net.Pipe()
	defer loser.Close()
	defer loserPeer.Close()
	defer winner.Close()
	defer winnerPeer.Close()
	winnerReady, loserCanceled, excess := make(chan struct{}), make(chan struct{}), make(chan struct{}, 2)
	winnerGate, loserGate := make(chan struct{}), make(chan struct{})
	var winnerOnce, loserOnce sync.Once
	releaseWinner := func() { winnerOnce.Do(func() { close(winnerGate) }) }
	releaseLoser := func() { loserOnce.Do(func() { close(loserGate) }) }
	defer releaseWinner()
	defer releaseLoser()
	var active, peak atomic.Int32
	type outcome struct {
		conn net.Conn
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		conn, err := FathomryRaceTCP(ctx, []net.IP{net.IPv4(127, 0, 0, 1), net.IPv4(127, 0, 0, 2), net.IPv4(127, 0, 0, 3), net.IPv4(127, 0, 0, 4)}, "443", 2, 5*time.Millisecond, func(ctx context.Context, _, address string) (net.Conn, error) {
			count := active.Add(1)
			defer active.Add(-1)
			for previous := peak.Load(); count > previous && !peak.CompareAndSwap(previous, count); previous = peak.Load() {
			}
			switch address {
			case "127.0.0.1:443":
				<-ctx.Done()
				close(loserCanceled)
				<-loserGate
				return loser, nil
			case "127.0.0.2:443":
				close(winnerReady)
				<-winnerGate
				return winner, nil
			default:
				excess <- struct{}{}
				<-ctx.Done()
				return nil, ctx.Err()
			}
		})
		done <- outcome{conn, err}
	}()
	select {
	case <-winnerReady:
	case <-ctx.Done():
		t.Fatal("second address was not raced")
	}
	select {
	case <-excess:
		t.Fatal("race exceeded candidate concurrency")
	case <-time.After(25 * time.Millisecond):
	}
	releaseWinner()
	select {
	case <-loserCanceled:
	case <-ctx.Done():
		t.Fatal("loser context not canceled")
	}
	select {
	case <-done:
		t.Fatal("race returned before late native dial terminated")
	default:
	}
	releaseLoser()
	select {
	case result := <-done:
		if result.err != nil || result.conn != winner || peak.Load() != 2 || active.Load() != 0 {
			t.Fatal("race outcome or bound changed", result.err, peak.Load(), active.Load())
		}
	case <-ctx.Done():
		t.Fatal("race did not join")
	}
	_ = loserPeer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := loserPeer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatal("late successful loser socket survived", err)
	}
}
