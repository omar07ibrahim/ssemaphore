//go:build linux

package main

import (
	"os"
	"syscall"
)

// A nonblocking leaf open prevents an attacker-controlled FIFO swap from
// stalling the Linux evidence renderer between Lstat and OpenFile. The
// subsequent fstat/snapshot checks still require the opened object to be the
// original regular file.
func openVisualReadFile(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}
