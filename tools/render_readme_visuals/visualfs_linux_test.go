//go:build linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

func TestVisualOutputBoundariesRejectTraversalLinksAndSpecialFiles(t *testing.T) {
	t.Run("artifact traversal", func(t *testing.T) {
		output := t.TempDir()
		escape := filepath.Join(output, "..", "escaped-visual.svg")
		artifacts := visualBoundaryArtifacts()
		artifacts["../escaped-visual.svg"] = []byte("<svg/>\n")
		if err := writeVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted traversal")
		}
		if _, err := checkVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("checkVisualArtifacts() accepted traversal")
		}
		if _, err := os.Lstat(escape); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("traversal target exists or cannot be inspected: %v", err)
		}
	})

	t.Run("symlink output component", func(t *testing.T) {
		parent := t.TempDir()
		realOutput := filepath.Join(parent, "real")
		if err := os.Mkdir(realOutput, 0o755); err != nil {
			t.Fatalf("mkdir real output: %v", err)
		}
		linkedOutput := filepath.Join(parent, "linked")
		if err := os.Symlink(realOutput, linkedOutput); err != nil {
			t.Fatalf("symlink output: %v", err)
		}
		artifacts := visualBoundaryArtifacts()
		if err := writeVisualArtifacts(linkedOutput, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted a symlink output")
		}
		if _, err := checkVisualArtifacts(linkedOutput, artifacts); err == nil {
			t.Fatal("checkVisualArtifacts() accepted a symlink output")
		}
	})

	t.Run("symlink ancestor component", func(t *testing.T) {
		parent := t.TempDir()
		realParent := filepath.Join(parent, "real")
		if err := os.Mkdir(realParent, 0o755); err != nil {
			t.Fatalf("mkdir real parent: %v", err)
		}
		linkedParent := filepath.Join(parent, "linked")
		if err := os.Symlink(realParent, linkedParent); err != nil {
			t.Fatalf("symlink parent: %v", err)
		}
		output := filepath.Join(linkedParent, "generated")
		if err := writeVisualArtifacts(output, visualBoundaryArtifacts()); err == nil {
			t.Fatal("writeVisualArtifacts() accepted a symlink ancestor")
		}
		if _, err := os.Lstat(filepath.Join(realParent, "generated")); !errors.Is(
			err,
			os.ErrNotExist,
		) {
			t.Fatalf("created output through symlink ancestor: %v", err)
		}
	})

	t.Run("managed symlink", func(t *testing.T) {
		output := t.TempDir()
		outside := filepath.Join(t.TempDir(), "outside.svg")
		original := []byte("outside remains unchanged\n")
		if err := os.WriteFile(outside, original, 0o644); err != nil {
			t.Fatalf("write outside fixture: %v", err)
		}
		if err := os.Symlink(outside, filepath.Join(output, "result.svg")); err != nil {
			t.Fatalf("symlink managed artifact: %v", err)
		}
		artifacts := visualBoundaryArtifacts()
		if _, err := checkVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("checkVisualArtifacts() accepted a managed symlink")
		}
		if err := writeVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted a managed symlink")
		}
		after, err := os.ReadFile(outside)
		if err != nil {
			t.Fatalf("read outside fixture: %v", err)
		}
		if !bytes.Equal(after, original) {
			t.Fatal("managed symlink changed its external target")
		}
	})

	t.Run("managed fifo", func(t *testing.T) {
		output := t.TempDir()
		if err := syscall.Mkfifo(filepath.Join(output, "result.svg"), 0o600); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		artifacts := visualBoundaryArtifacts()
		if _, err := checkVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("checkVisualArtifacts() accepted a FIFO")
		}
		if err := writeVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted a FIFO")
		}
	})

	t.Run("oversized managed artifact", func(t *testing.T) {
		output := t.TempDir()
		target := filepath.Join(output, "result.svg")
		file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("create oversized artifact: %v", err)
		}
		if err := file.Truncate(visualMaxArtifactBytes + 1); err != nil {
			_ = file.Close()
			t.Fatalf("truncate oversized artifact: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close oversized artifact: %v", err)
		}
		artifacts := visualBoundaryArtifacts()
		if _, err := checkVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("checkVisualArtifacts() accepted an oversized artifact")
		}
		if err := writeVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted an oversized existing artifact")
		}

		artifacts = visualBoundaryArtifacts()
		artifacts["result.svg"] = make([]byte, visualMaxArtifactBytes+1)
		if err := writeVisualArtifacts(t.TempDir(), artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted an oversized payload")
		}
	})
}

func TestVisualOutputInventoryIsBoundedExclusiveAndModeChecked(t *testing.T) {
	t.Run("unexpected nested directory", func(t *testing.T) {
		output := t.TempDir()
		artifacts := visualBoundaryArtifacts()
		if err := writeVisualArtifacts(output, artifacts); err != nil {
			t.Fatalf("write initial bundle: %v", err)
		}
		if err := os.Mkdir(filepath.Join(output, "nested"), 0o755); err != nil {
			t.Fatalf("mkdir unexpected nested directory: %v", err)
		}
		stale, err := checkVisualArtifacts(output, artifacts)
		if err != nil {
			t.Fatalf("check nested directory: %v", err)
		}
		if !slices.Equal(stale, []string{"unexpected:nested"}) {
			t.Fatalf("stale = %v, want unexpected nested directory", stale)
		}
		if err := writeVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted an unexpected directory")
		}
	})

	t.Run("entry count", func(t *testing.T) {
		output := t.TempDir()
		for index := range visualMaxOutputEntries + 1 {
			name := filepath.Join(output, fmt.Sprintf("unexpected-%03d.txt", index))
			if err := os.WriteFile(name, nil, 0o644); err != nil {
				t.Fatalf("write entry %d: %v", index, err)
			}
		}
		artifacts := visualBoundaryArtifacts()
		if _, err := checkVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("checkVisualArtifacts() accepted an over-wide directory")
		}
		if err := writeVisualArtifacts(output, artifacts); err == nil {
			t.Fatal("writeVisualArtifacts() accepted an over-wide directory")
		}
	})

	t.Run("exact artifact mode", func(t *testing.T) {
		output := t.TempDir()
		artifacts := visualBoundaryArtifacts()
		if err := writeVisualArtifacts(output, artifacts); err != nil {
			t.Fatalf("write initial bundle: %v", err)
		}
		target := filepath.Join(output, "result.svg")
		if err := os.Chmod(target, 0o600); err != nil {
			t.Fatalf("chmod artifact: %v", err)
		}
		stale, err := checkVisualArtifacts(output, artifacts)
		if err != nil {
			t.Fatalf("check wrong mode: %v", err)
		}
		if !slices.Equal(stale, []string{"result.svg"}) {
			t.Fatalf("stale = %v, want wrong-mode result.svg", stale)
		}
		if err := writeVisualArtifacts(output, artifacts); err != nil {
			t.Fatalf("rewrite wrong-mode artifact: %v", err)
		}
		info, err := os.Lstat(target)
		if err != nil {
			t.Fatalf("lstat repaired artifact: %v", err)
		}
		if info.Mode() != 0o644 {
			t.Fatalf("repaired mode = %v, want 0644", info.Mode())
		}
		stale, err = checkVisualArtifacts(output, artifacts)
		if err != nil || len(stale) != 0 {
			t.Fatalf("repaired check = (%v, %v), want current", stale, err)
		}
	})
}

func TestVisualSourceBoundariesRejectTraversalLinksSpecialAndOversizedFiles(
	t *testing.T,
) {
	t.Run("source traversal", func(t *testing.T) {
		root := t.TempDir()
		outside := filepath.Join(root, "..", "outside.go")
		if err := os.WriteFile(outside, []byte("package outside\n"), 0o644); err != nil {
			t.Fatalf("write outside source: %v", err)
		}
		if _, err := digestVisualSources(
			root,
			[]string{"../outside.go"},
			nil,
			false,
		); err == nil {
			t.Fatal("digestVisualSources() accepted traversal")
		}
	})

	t.Run("source directory symlink", func(t *testing.T) {
		root := t.TempDir()
		realDirectory := filepath.Join(root, "real")
		if err := os.Mkdir(realDirectory, 0o755); err != nil {
			t.Fatalf("mkdir source: %v", err)
		}
		if err := os.WriteFile(
			filepath.Join(realDirectory, "source.go"),
			[]byte("package source\n"),
			0o644,
		); err != nil {
			t.Fatalf("write source: %v", err)
		}
		if err := os.Symlink(realDirectory, filepath.Join(root, "linked")); err != nil {
			t.Fatalf("symlink source directory: %v", err)
		}
		if _, err := digestVisualSources(root, nil, []string{"linked"}, false); err == nil {
			t.Fatal("digestVisualSources() accepted a symlink directory")
		}
	})

	t.Run("source file symlink", func(t *testing.T) {
		root := t.TempDir()
		realSource := filepath.Join(root, "real.go")
		if err := os.WriteFile(realSource, []byte("package source\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		if err := os.Symlink(realSource, filepath.Join(root, "linked.go")); err != nil {
			t.Fatalf("symlink source: %v", err)
		}
		if _, err := digestVisualSources(
			root,
			[]string{"linked.go"},
			nil,
			false,
		); err == nil {
			t.Fatal("digestVisualSources() accepted a symlink file")
		}
	})

	t.Run("walked symlink entry", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "source")
		if err := os.Mkdir(source, 0o755); err != nil {
			t.Fatalf("mkdir source tree: %v", err)
		}
		realSource := filepath.Join(root, "real.go")
		if err := os.WriteFile(realSource, []byte("package source\n"), 0o644); err != nil {
			t.Fatalf("write source: %v", err)
		}
		if err := os.Symlink(realSource, filepath.Join(source, "linked.txt")); err != nil {
			t.Fatalf("symlink walked entry: %v", err)
		}
		if _, err := digestVisualSources(root, nil, []string{"source"}, false); err == nil {
			t.Fatal("source walk accepted a symlink entry")
		}
	})

	t.Run("source fifo", func(t *testing.T) {
		root := t.TempDir()
		if err := syscall.Mkfifo(filepath.Join(root, "source.go"), 0o600); err != nil {
			t.Fatalf("mkfifo: %v", err)
		}
		if _, err := digestVisualSources(
			root,
			[]string{"source.go"},
			nil,
			false,
		); err == nil {
			t.Fatal("digestVisualSources() accepted a FIFO")
		}
		if _, err := digestVisualSources(root, nil, []string{"."}, false); err == nil {
			t.Fatal("source walk accepted a FIFO")
		}
	})

	t.Run("oversized source", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(root, "source.go")
		file, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatalf("create oversized source: %v", err)
		}
		if err := file.Truncate(visualMaxSourceFileBytes + 1); err != nil {
			_ = file.Close()
			t.Fatalf("truncate oversized source: %v", err)
		}
		if err := file.Close(); err != nil {
			t.Fatalf("close oversized source: %v", err)
		}
		if _, err := digestVisualSources(
			root,
			[]string{"source.go"},
			nil,
			false,
		); err == nil {
			t.Fatal("digestVisualSources() accepted an oversized source")
		}
	})

	t.Run("source directory entries", func(t *testing.T) {
		root := t.TempDir()
		source := filepath.Join(root, "source")
		if err := os.Mkdir(source, 0o755); err != nil {
			t.Fatalf("mkdir source tree: %v", err)
		}
		for index := range visualMaxSourceDirectoryEntries + 1 {
			name := filepath.Join(source, fmt.Sprintf("file-%03d.txt", index))
			if err := os.WriteFile(name, nil, 0o644); err != nil {
				t.Fatalf("write source entry %d: %v", index, err)
			}
		}
		if _, err := digestVisualSources(root, nil, []string{"source"}, false); err == nil {
			t.Fatal("digestVisualSources() accepted an over-wide source directory")
		}
	})
}

func TestVisualReadRejectsRegularFileSwappedForFIFOWithoutBlocking(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "source.go")
	if err := os.WriteFile(target, []byte("package source\n"), 0o644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	root, err := openPinnedVisualDirectory(directory, false)
	if err != nil {
		t.Fatalf("open source root: %v", err)
	}
	defer root.Close()

	result := make(chan error, 1)
	go func() {
		_, _, readErr := readBoundedVisualLeafWithHook(
			root,
			"source.go",
			visualMaxSourceFileBytes,
			func() error {
				if err := os.Remove(target); err != nil {
					return err
				}
				return syscall.Mkfifo(target, 0o600)
			},
		)
		result <- readErr
	}()

	select {
	case readErr := <-result:
		if readErr == nil {
			t.Fatal("readBoundedVisualLeafWithHook() accepted a swapped FIFO")
		}
	case <-time.After(2 * time.Second):
		writer, openErr := os.OpenFile(
			target,
			os.O_WRONLY|syscall.O_NONBLOCK,
			0,
		)
		if openErr == nil {
			_ = writer.Close()
		}
		t.Fatal("readBoundedVisualLeafWithHook() blocked on a swapped FIFO")
	}
}

func TestAtomicWriteVisualInternalFailureCleansTemporaryFiles(t *testing.T) {
	for _, failAt := range []string{
		"after-create",
		"after-write",
		"after-sync",
		"before-rename",
	} {
		t.Run(failAt, func(t *testing.T) {
			output := t.TempDir()
			target := filepath.Join(output, "result.svg")
			original := []byte("old result\n")
			if err := os.WriteFile(target, original, 0o644); err != nil {
				t.Fatalf("write original artifact: %v", err)
			}
			root, err := openPinnedVisualDirectory(output, false)
			if err != nil {
				t.Fatalf("open output root: %v", err)
			}
			writeErr := atomicWriteVisualWithHook(
				root,
				"result.svg",
				[]byte("new result\n"),
				func(stage string) error {
					if stage == failAt {
						return errors.New("injected failure")
					}
					return nil
				},
			)
			closeErr := root.Close()
			if writeErr == nil {
				t.Fatal("atomicWriteVisualWithHook() ignored injected failure")
			}
			if closeErr != nil {
				t.Fatalf("close output root: %v", closeErr)
			}
			got, err := os.ReadFile(target)
			if err != nil {
				t.Fatalf("read original artifact: %v", err)
			}
			if !bytes.Equal(got, original) {
				t.Fatalf("target = %q, want original %q", got, original)
			}
			entries, err := os.ReadDir(output)
			if err != nil {
				t.Fatalf("read output entries: %v", err)
			}
			if len(entries) != 1 || entries[0].Name() != "result.svg" {
				t.Fatalf("output entries = %v, want only result.svg", entries)
			}
		})
	}
}

func TestVisualBundleFailureLeavesManifestStale(t *testing.T) {
	tests := []struct {
		name       string
		failAt     string
		wantCalls  []string
		wantStale  []string
		wantResult map[string][]byte
	}{
		{
			name:      "non-manifest write",
			failAt:    "b.svg",
			wantCalls: []string{"a.txt", "b.svg"},
			wantStale: []string{"b.svg", visualManifestName},
			wantResult: map[string][]byte{
				"a.txt":            []byte("new a\n"),
				"b.svg":            []byte("old b\n"),
				visualManifestName: []byte("old manifest\n"),
			},
		},
		{
			name:      "manifest write",
			failAt:    visualManifestName,
			wantCalls: []string{"a.txt", "b.svg", visualManifestName},
			wantStale: []string{visualManifestName},
			wantResult: map[string][]byte{
				"a.txt":            []byte("new a\n"),
				"b.svg":            []byte("new b\n"),
				visualManifestName: []byte("old manifest\n"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			output := t.TempDir()
			oldArtifacts := map[string][]byte{
				"a.txt":            []byte("old a\n"),
				"b.svg":            []byte("old b\n"),
				visualManifestName: []byte("old manifest\n"),
			}
			newArtifacts := map[string][]byte{
				"a.txt":            []byte("new a\n"),
				"b.svg":            []byte("new b\n"),
				visualManifestName: []byte("new manifest\n"),
			}
			if err := writeVisualArtifacts(output, oldArtifacts); err != nil {
				t.Fatalf("write old bundle: %v", err)
			}
			root, err := openPinnedVisualDirectory(output, false)
			if err != nil {
				t.Fatalf("open output root: %v", err)
			}
			var calls []string
			writer := func(root *os.Root, name string, payload []byte) error {
				calls = append(calls, name)
				if name == test.failAt {
					return errors.New("injected write failure")
				}
				return atomicWriteVisual(root, name, payload)
			}
			writeErr := writeVisualArtifactBundle(root, newArtifacts, writer)
			closeErr := root.Close()
			if writeErr == nil {
				t.Fatal("writeVisualArtifactBundle() ignored injected failure")
			}
			if closeErr != nil {
				t.Fatalf("close output root: %v", closeErr)
			}
			if !slices.Equal(calls, test.wantCalls) {
				t.Fatalf("write calls = %v, want %v", calls, test.wantCalls)
			}
			for name, want := range test.wantResult {
				got, err := os.ReadFile(filepath.Join(output, name))
				if err != nil {
					t.Fatalf("read %s: %v", name, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("%s = %q, want %q", name, got, want)
				}
			}
			stale, err := checkVisualArtifacts(output, newArtifacts)
			if err != nil {
				t.Fatalf("check partial bundle: %v", err)
			}
			if !slices.Equal(stale, test.wantStale) {
				t.Fatalf("stale = %v, want %v", stale, test.wantStale)
			}
			entries, err := os.ReadDir(output)
			if err != nil {
				t.Fatalf("read output entries: %v", err)
			}
			for _, entry := range entries {
				if entry.Name()[0] == '.' {
					t.Fatalf("temporary artifact remained: %s", entry.Name())
				}
			}
		})
	}
}

func visualBoundaryArtifacts() map[string][]byte {
	return map[string][]byte{
		"result.svg":       []byte("<svg/>\n"),
		visualManifestName: []byte("{}\n"),
	}
}
