package app

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const maxPolicyPathBytes = 4096

var errPolicyReadFailed = errors.New("policy file could not be read")

// readPolicyFile deliberately accepts only a private, caller-owned regular file.
// The syscall flags and ownership checks make this loader Linux-specific.
func readPolicyFile(path string) ([]byte, error) {
	if !validPolicyPath(path) {
		return nil, errPolicyReadFailed
	}

	effectiveUID := uint32(os.Geteuid())
	if !safePolicyAncestors(path, effectiveUID) {
		return nil, errPolicyReadFailed
	}

	before, err := os.Lstat(path)
	if err != nil || !safePolicyFileInfo(before, effectiveUID) {
		return nil, errPolicyReadFailed
	}

	fd, err := syscall.Open(
		path,
		syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK,
		0,
	)
	if err != nil {
		return nil, errPolicyReadFailed
	}

	file := os.NewFile(uintptr(fd), "policy")
	if file == nil {
		_ = syscall.Close(fd)
		return nil, errPolicyReadFailed
	}

	data, readOK := readOpenedPolicyFile(file, before, effectiveUID)
	closeOK := file.Close() == nil
	if !readOK || !closeOK {
		return nil, errPolicyReadFailed
	}
	return data, nil
}

func validPolicyPath(path string) bool {
	return path != "" &&
		len(path) <= maxPolicyPathBytes &&
		!strings.ContainsRune(path, '\x00') &&
		filepath.IsAbs(path) &&
		filepath.Clean(path) == path
}

func safePolicyAncestors(path string, effectiveUID uint32) bool {
	for current := filepath.Dir(path); ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return false
		}

		owner, ok := policyOwner(info)
		if !ok || (owner != 0 && owner != effectiveUID) {
			return false
		}

		// A root-owned sticky directory is the one writable ancestor that is
		// safe to traverse; this admits standard Linux temporary roots such as
		// /tmp while every descendant remains subject to the stricter rule.
		if info.Mode().Perm()&0o022 != 0 &&
			!(owner == 0 && info.Mode()&os.ModeSticky != 0) {
			return false
		}

		parent := filepath.Dir(current)
		if parent == current {
			return true
		}
	}
}

func safePolicyFileInfo(info os.FileInfo, effectiveUID uint32) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode() != 0o600 {
		return false
	}
	if info.Size() <= 0 || info.Size() > int64(maxPolicyBytes) {
		return false
	}

	owner, ok := policyOwner(info)
	return ok && owner == effectiveUID
}

func policyOwner(info os.FileInfo) (uint32, bool) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return stat.Uid, true
}

func readOpenedPolicyFile(
	file *os.File,
	before os.FileInfo,
	effectiveUID uint32,
) ([]byte, bool) {
	afterOpen, err := file.Stat()
	if err != nil || !safePolicyFileInfo(afterOpen, effectiveUID) ||
		!os.SameFile(before, afterOpen) || before.Size() != afterOpen.Size() {
		return nil, false
	}

	data, err := io.ReadAll(io.LimitReader(file, int64(maxPolicyBytes)+1))
	if err != nil || len(data) == 0 || len(data) > maxPolicyBytes {
		return nil, false
	}

	afterRead, err := file.Stat()
	if err != nil || !safePolicyFileInfo(afterRead, effectiveUID) ||
		!os.SameFile(afterOpen, afterRead) ||
		afterRead.Size() != afterOpen.Size() ||
		afterRead.Size() != int64(len(data)) {
		return nil, false
	}
	return data, true
}
