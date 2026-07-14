package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestReadPolicyFileAcceptsPrivateRegularFile(t *testing.T) {
	path := writePolicyFixture(t, []byte(`{"schema_version":1}`))

	got, err := readPolicyFile(path)
	if err != nil {
		t.Fatalf("readPolicyFile() error = %v", err)
	}
	if want := []byte(`{"schema_version":1}`); !bytes.Equal(got, want) {
		t.Fatalf("readPolicyFile() = %q, want %q", got, want)
	}
}

func TestReadPolicyFileRejectsInvalidPaths(t *testing.T) {
	directory := securePolicyTempDir(t)
	validPath := filepath.Join(directory, "policy.json")
	if err := os.WriteFile(validPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write policy fixture: %v", err)
	}

	tests := map[string]string{
		"empty":     "",
		"relative":  "policy.json",
		"unclean":   directory + string(os.PathSeparator) + "." + string(os.PathSeparator) + "policy.json",
		"nul":       validPath + "\x00canary",
		"too long":  string(os.PathSeparator) + strings.Repeat("a", maxPolicyPathBytes),
		"directory": directory,
	}
	for name, path := range tests {
		t.Run(name, func(t *testing.T) {
			requirePolicyReadFailure(t, path)
		})
	}
}

func TestReadPolicyFileRejectsPermissiveMode(t *testing.T) {
	path := writePolicyFixture(t, []byte("private"))
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod policy fixture: %v", err)
	}

	requirePolicyReadFailure(t, path)
}

func TestReadPolicyFileRejectsFinalSymlink(t *testing.T) {
	directory := securePolicyTempDir(t)
	target := filepath.Join(directory, "target.json")
	if err := os.WriteFile(target, []byte("private"), 0o600); err != nil {
		t.Fatalf("write policy target: %v", err)
	}
	link := filepath.Join(directory, "policy.json")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("create policy symlink: %v", err)
	}

	requirePolicyReadFailure(t, link)
}

func TestReadPolicyFileRejectsSymlinkComponent(t *testing.T) {
	directory := securePolicyTempDir(t)
	realDirectory := filepath.Join(directory, "real")
	if err := os.Mkdir(realDirectory, 0o700); err != nil {
		t.Fatalf("create real directory: %v", err)
	}
	policy := filepath.Join(realDirectory, "policy.json")
	if err := os.WriteFile(policy, []byte("private"), 0o600); err != nil {
		t.Fatalf("write policy fixture: %v", err)
	}
	link := filepath.Join(directory, "linked")
	if err := os.Symlink(realDirectory, link); err != nil {
		t.Fatalf("create directory symlink: %v", err)
	}

	requirePolicyReadFailure(t, filepath.Join(link, "policy.json"))
}

func TestReadPolicyFileRejectsFIFOWithoutBlocking(t *testing.T) {
	directory := securePolicyTempDir(t)
	path := filepath.Join(directory, "policy.pipe")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("create FIFO fixture: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, err := readPolicyFile(path)
		result <- err
	}()

	select {
	case err := <-result:
		if err != errPolicyReadFailed {
			t.Fatalf("readPolicyFile() error = %v, want exact static error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("readPolicyFile() blocked while opening a FIFO")
	}
}

func TestReadPolicyFileRejectsEmptyFile(t *testing.T) {
	path := writePolicyFixture(t, nil)
	requirePolicyReadFailure(t, path)
}

func TestReadPolicyFileEnforcesExactSizeLimit(t *testing.T) {
	t.Run("maximum", func(t *testing.T) {
		content := bytes.Repeat([]byte{'x'}, maxPolicyBytes)
		path := writePolicyFixture(t, content)

		got, err := readPolicyFile(path)
		if err != nil {
			t.Fatalf("readPolicyFile() at size limit error = %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Fatal("readPolicyFile() changed content at the size limit")
		}
	})

	t.Run("one byte over", func(t *testing.T) {
		path := writePolicyFixture(t, bytes.Repeat([]byte{'x'}, maxPolicyBytes+1))
		requirePolicyReadFailure(t, path)
	})
}

func TestReadPolicyFileRejectsUnsafeAncestor(t *testing.T) {
	directory := securePolicyTempDir(t)
	unsafeDirectory := filepath.Join(directory, "unsafe")
	if err := os.Mkdir(unsafeDirectory, 0o700); err != nil {
		t.Fatalf("create unsafe ancestor fixture: %v", err)
	}
	if err := os.Chmod(unsafeDirectory, 0o777); err != nil {
		t.Fatalf("chmod unsafe ancestor fixture: %v", err)
	}
	path := filepath.Join(unsafeDirectory, "policy.json")
	if err := os.WriteFile(path, []byte("private"), 0o600); err != nil {
		t.Fatalf("write policy fixture: %v", err)
	}

	requirePolicyReadFailure(t, path)
}

func TestReadPolicyFileRejectsDifferentOwnerWhenPrivileged(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing a fixture owner requires root")
	}

	path := writePolicyFixture(t, []byte("private"))
	const unprivilegedUID = 65534
	if err := os.Chown(path, unprivilegedUID, -1); err != nil {
		t.Fatalf("chown policy fixture: %v", err)
	}

	requirePolicyReadFailure(t, path)
}

func TestReadPolicyFileRejectsSizeChange(t *testing.T) {
	path := writePolicyFixture(t, []byte("before"))
	before, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat policy fixture: %v", err)
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatalf("open policy fixture: %v", err)
	}
	defer file.Close()
	if err := os.WriteFile(path, []byte("after-size-change"), 0o600); err != nil {
		t.Fatalf("resize policy fixture: %v", err)
	}

	if data, ok := readOpenedPolicyFile(file, before, uint32(os.Geteuid())); ok || data != nil {
		t.Fatal("readOpenedPolicyFile() accepted a file whose size changed")
	}
}

func TestReadPolicyFileReturnsOnlyStaticError(t *testing.T) {
	const canary = "DO_NOT_DISCLOSE_POLICY_PATH_OR_CONTENT"
	path := filepath.Join(securePolicyTempDir(t), canary)

	data, err := readPolicyFile(path)
	if data != nil {
		t.Fatalf("readPolicyFile() data = %q, want nil", data)
	}
	if err != errPolicyReadFailed {
		t.Fatalf("readPolicyFile() error = %v, want exact static error", err)
	}
	if strings.Contains(err.Error(), canary) || strings.Contains(err.Error(), path) {
		t.Fatalf("readPolicyFile() error disclosed a canary: %q", err)
	}
}

func securePolicyTempDir(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("chmod temporary directory: %v", err)
	}
	return directory
}

func writePolicyFixture(t *testing.T, content []byte) string {
	t.Helper()
	directory := securePolicyTempDir(t)
	path := filepath.Join(directory, "policy.json")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write policy fixture: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod policy fixture: %v", err)
	}
	return path
}

func requirePolicyReadFailure(t *testing.T, path string) {
	t.Helper()
	data, err := readPolicyFile(path)
	if data != nil {
		t.Fatalf("readPolicyFile() data = %q, want nil", data)
	}
	if err != errPolicyReadFailed {
		t.Fatalf("readPolicyFile() error = %v, want exact static error", err)
	}
}
