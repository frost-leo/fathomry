//go:build unix

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

package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSignalChild(t *testing.T) {
	if os.Getenv("FATHOMRY_SIGNAL_CHILD") != "1" {
		return
	}
	status := runProcess(nil, os.Stdin, os.Stdout, os.Stderr,
		func(ctx context.Context, _ []string, _ io.Reader, _, _ io.Writer) int {
			_, _ = io.WriteString(os.Stderr, "READY\n")
			<-ctx.Done()
			return 130
		})
	os.Exit(status)
}

func TestCleanupChild(t *testing.T) {
	if os.Getenv("FATHOMRY_CLEANUP_CHILD") != "1" {
		return
	}
	if status := runProcess(nil, os.Stdin, os.Stdout, os.Stderr,
		func(context.Context, []string, io.Reader, io.Writer, io.Writer) int { return 1 }); status != 1 {
		os.Exit(99)
	}
	_, _ = io.WriteString(os.Stderr, "READY\n")
	time.Sleep(5 * time.Second)
	os.Exit(98)
}

func TestUnixSignalsAndBrokenPipe(t *testing.T) {
	for _, test := range []struct {
		name   string
		signal syscall.Signal
		status int
	}{
		{"interrupt", syscall.SIGINT, 130},
		{"termination", syscall.SIGTERM, 143},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSignalChild$")
			command.Env = append(os.Environ(), "FATHOMRY_SIGNAL_CHILD=1")
			stderr, err := command.StderrPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			ready, err := bufio.NewReader(stderr).ReadString('\n')
			if err != nil || ready != "READY\n" {
				_ = command.Process.Kill()
				t.Fatalf("child readiness: %q %v", ready, err)
			}
			if err := command.Process.Signal(test.signal); err != nil {
				t.Fatal(err)
			}
			err = command.Wait()
			if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != test.status || ctx.Err() != nil {
				t.Fatalf("signal %v: exit=%v context=%v", test.signal, err, ctx.Err())
			}
		})
	}
	t.Run("registration-cleanup-after-error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCleanupChild$")
		command.Env = append(os.Environ(), "FATHOMRY_CLEANUP_CHILD=1")
		stderr, err := command.StderrPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		ready, err := bufio.NewReader(stderr).ReadString('\n')
		if err != nil || ready != "READY\n" {
			_ = command.Process.Kill()
			t.Fatalf("cleanup child readiness: %q %v", ready, err)
		}
		if err := command.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		err = command.Wait()
		exit, ok := err.(*exec.ExitError)
		if ctx.Err() != nil || !ok || exit.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGTERM {
			t.Fatalf("signal registration not restored: %v context=%v", err, ctx.Err())
		}
	})
	path := buildCLI(t, "", true)
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = read.Close()
	defer write.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "version", "--output=json")
	command.Stdout = write
	var diagnostic strings.Builder
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	err = command.Wait()
	if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 1 || !strings.Contains(diagnostic.String(), "Could not write") || ctx.Err() != nil {
		t.Fatalf("broken pipe: exit=%v stderr=%q context=%v", err, diagnostic.String(), ctx.Err())
	}
}

func TestSecondInterruptStopsBlockedWriter(t *testing.T) {
	path := buildCLI(t, "", true)
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer read.Close()
	defer write.Close()
	fd := int(write.Fd())
	if err := syscall.SetNonblock(fd, true); err != nil {
		t.Fatal(err)
	}
	filled := false
	buffer := make([]byte, 8192)
	for total := 0; total < 1<<20; {
		count, err := syscall.Write(fd, buffer)
		if err == syscall.EAGAIN {
			filled = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total += count
	}
	if !filled {
		t.Fatal("could not fill test pipe")
	}
	if err := syscall.SetNonblock(fd, false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "--help")
	command.Stdout = write
	var diagnostic strings.Builder
	command.Stderr = &diagnostic
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = write.Close()
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	time.Sleep(150 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		t.Fatalf("first interrupt unexpectedly stopped blocked writer: %v stderr=%q", err, diagnostic.String())
	case <-time.After(150 * time.Millisecond):
	}
	if err := command.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if err == nil || ctx.Err() != nil {
			t.Fatalf("second interrupt exit=%v context=%v stderr=%q", err, ctx.Err(), diagnostic.String())
		}
	case <-ctx.Done():
		t.Fatalf("second interrupt did not stop blocked writer: %v stderr=%q", ctx.Err(), diagnostic.String())
	}
}
