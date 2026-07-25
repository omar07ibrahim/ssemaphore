//go:build !linux

package main

import "os"

func openVisualReadFile(root *os.Root, name string) (*os.File, error) {
	return root.Open(name)
}
