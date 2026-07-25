package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

const (
	visualMaxArtifactBytes          int64 = 2 << 20
	visualMaxArtifactCount                = 64
	visualMaxArtifactNameBytes            = 96
	visualMaxOutputEntries                = 128
	visualMaxSourceFileBytes        int64 = 4 << 20
	visualMaxSourceTotalBytes       int64 = 64 << 20
	visualMaxSourceEntries                = 8192
	visualMaxSourceDirectoryEntries       = 512
	visualMaxSourceDepth                  = 64
	visualTemporaryAttempts               = 32
)

type visualArtifactWriter func(*os.Root, string, []byte) error

type visualAtomicWriteHook func(string) error

type visualReadHook func() error

type visualSourceSelection struct {
	files   map[string]struct{}
	entries int
}

// openPinnedVisualDirectory walks each absolute path component through os.Root.
// Every component is required to remain the same non-symlink directory while
// the next Root is opened, so callers operate on pinned directory handles.
func openPinnedVisualDirectory(name string, create bool) (*os.Root, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return nil, fmt.Errorf("resolve directory: %w", err)
	}
	absolute = filepath.Clean(absolute)
	volume := filepath.VolumeName(absolute)
	filesystemRoot := volume + string(filepath.Separator)
	if !filepath.IsAbs(absolute) || filesystemRoot == "" {
		return nil, errors.New("directory is not absolute")
	}

	current, err := os.OpenRoot(filesystemRoot)
	if err != nil {
		return nil, fmt.Errorf("open filesystem root: %w", err)
	}
	relative := strings.TrimPrefix(absolute, filesystemRoot)
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		if component == "" {
			continue
		}
		if component == "." || component == ".." ||
			strings.ContainsAny(component, `/\`) {
			_ = current.Close()
			return nil, errors.New("invalid directory component")
		}
		next, childErr := openVisualChildDirectory(current, component, create)
		if childErr != nil {
			_ = current.Close()
			return nil, childErr
		}
		if err := current.Close(); err != nil {
			_ = next.Close()
			return nil, errors.New("close parent directory")
		}
		current = next
	}
	return current, nil
}

func openVisualRelativeDirectory(
	root *os.Root,
	name string,
	create bool,
) (*os.Root, error) {
	canonical, err := canonicalVisualRelativePath(name, true)
	if err != nil {
		return nil, err
	}
	current, err := root.OpenRoot(".")
	if err != nil {
		return nil, fmt.Errorf("clone root directory: %w", err)
	}
	rootInfo, rootErr := root.Stat(".")
	currentInfo, currentErr := current.Stat(".")
	if rootErr != nil || currentErr != nil ||
		!rootInfo.IsDir() || !currentInfo.IsDir() ||
		!sameVisualFileSnapshot(rootInfo, currentInfo) {
		_ = current.Close()
		return nil, errors.New("root directory changed")
	}
	if canonical == "." {
		return current, nil
	}
	for _, component := range strings.Split(canonical, "/") {
		next, childErr := openVisualChildDirectory(current, component, create)
		if childErr != nil {
			_ = current.Close()
			return nil, childErr
		}
		if err := current.Close(); err != nil {
			_ = next.Close()
			return nil, errors.New("close parent directory")
		}
		current = next
	}
	return current, nil
}

func openVisualChildDirectory(
	parent *os.Root,
	name string,
	create bool,
) (*os.Root, error) {
	created := false
	before, err := parent.Lstat(name)
	if errors.Is(err, os.ErrNotExist) && create {
		if err := parent.Mkdir(name, 0o755); err != nil &&
			!errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("create directory: %w", err)
		}
		created = true
		before, err = parent.Lstat(name)
	}
	if err != nil {
		return nil, fmt.Errorf("inspect directory: %w", err)
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, errors.New("directory component is not a real directory")
	}

	next, err := parent.OpenRoot(name)
	if err != nil {
		return nil, fmt.Errorf("open child directory: %w", err)
	}
	after, afterErr := parent.Lstat(name)
	opened, openedErr := next.Stat(".")
	if afterErr != nil || openedErr != nil ||
		after.Mode()&os.ModeSymlink != 0 ||
		!after.IsDir() || !opened.IsDir() ||
		!sameVisualFileSnapshot(before, after) ||
		!sameVisualFileSnapshot(before, opened) {
		_ = next.Close()
		return nil, errors.New("directory component changed")
	}
	if created {
		if err := syncVisualDirectory(parent); err != nil {
			_ = next.Close()
			return nil, errors.New("sync created directory parent")
		}
	}
	return next, nil
}

func canonicalVisualRelativePath(name string, allowDot bool) (string, error) {
	if name == "" || strings.Contains(name, `\`) || !fs.ValidPath(name) {
		return "", errors.New("invalid relative path")
	}
	if path.Clean(name) != name || (!allowDot && name == ".") {
		return "", errors.New("non-canonical relative path")
	}
	return name, nil
}

func readBoundedVisualFile(
	root *os.Root,
	name string,
	maxBytes int64,
) ([]byte, fs.FileInfo, error) {
	canonical, err := canonicalVisualRelativePath(name, false)
	if err != nil {
		return nil, nil, err
	}
	if maxBytes < 0 {
		return nil, nil, errors.New("invalid file size bound")
	}
	parent, err := openVisualRelativeDirectory(root, path.Dir(canonical), false)
	if err != nil {
		return nil, nil, err
	}
	payload, info, readErr := readBoundedVisualLeaf(
		parent,
		path.Base(canonical),
		maxBytes,
	)
	closeErr := parent.Close()
	if readErr != nil {
		return nil, nil, readErr
	}
	if closeErr != nil {
		return nil, nil, errors.New("close source directory")
	}
	return payload, info, nil
}

func readBoundedVisualLeaf(
	root *os.Root,
	name string,
	maxBytes int64,
) ([]byte, fs.FileInfo, error) {
	return readBoundedVisualLeafWithHook(root, name, maxBytes, nil)
}

func readBoundedVisualLeafWithHook(
	root *os.Root,
	name string,
	maxBytes int64,
	hook visualReadHook,
) ([]byte, fs.FileInfo, error) {
	if _, err := canonicalVisualRelativePath(name, false); err != nil ||
		path.Base(name) != name {
		return nil, nil, errors.New("invalid file name")
	}
	before, err := root.Lstat(name)
	if err != nil {
		return nil, nil, fmt.Errorf("inspect regular file: %w", err)
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, nil, errors.New("input is not a real regular file")
	}
	if before.Size() < 0 || before.Size() > maxBytes {
		return nil, nil, errors.New("regular file exceeds size bound")
	}

	if hook != nil {
		if err := hook(); err != nil {
			return nil, nil, errors.New("run regular file read hook")
		}
	}
	file, err := openVisualReadFile(root, name)
	if err != nil {
		return nil, nil, fmt.Errorf("open regular file: %w", err)
	}
	openedBefore, statErr := file.Stat()
	if statErr != nil ||
		!openedBefore.Mode().IsRegular() ||
		!sameVisualFileSnapshot(before, openedBefore) {
		_ = file.Close()
		return nil, nil, errors.New("regular file changed before open")
	}
	payload, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
	openedAfter, afterStatErr := file.Stat()
	closeErr := file.Close()
	if readErr != nil {
		return nil, nil, errors.New("read regular file")
	}
	if afterStatErr != nil || closeErr != nil {
		return nil, nil, errors.New("finish regular file read")
	}
	if int64(len(payload)) > maxBytes {
		return nil, nil, errors.New("regular file exceeds size bound")
	}
	after, lstatErr := root.Lstat(name)
	if lstatErr != nil ||
		after.Mode()&os.ModeSymlink != 0 ||
		!after.Mode().IsRegular() ||
		!sameVisualFileSnapshot(before, after) ||
		!sameVisualFileSnapshot(before, openedAfter) {
		return nil, nil, errors.New("regular file changed during read")
	}
	return payload, after, nil
}

func readBoundedVisualEntries(root *os.Root, limit int) ([]string, error) {
	if limit < 0 {
		return nil, errors.New("invalid directory entry bound")
	}
	before, err := root.Stat(".")
	if err != nil || !before.IsDir() {
		return nil, errors.New("inspect pinned directory")
	}
	directory, err := root.Open(".")
	if err != nil {
		return nil, fmt.Errorf("open pinned directory: %w", err)
	}
	opened, statErr := directory.Stat()
	if statErr != nil || !opened.IsDir() ||
		!sameVisualFileSnapshot(before, opened) {
		_ = directory.Close()
		return nil, errors.New("pinned directory changed before scan")
	}
	names, readErr := directory.Readdirnames(limit + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, errors.New("read directory entries")
	}
	if closeErr != nil {
		return nil, errors.New("close directory scan")
	}
	if len(names) > limit || (len(names) == limit+1 && readErr == nil) {
		return nil, errors.New("directory exceeds entry bound")
	}
	after, err := root.Stat(".")
	if err != nil || !after.IsDir() ||
		!sameVisualFileSnapshot(before, after) {
		return nil, errors.New("pinned directory changed during scan")
	}
	for _, name := range names {
		if name == "" || name == "." || name == ".." ||
			strings.ContainsAny(name, `/\`) {
			return nil, errors.New("directory contains an invalid entry name")
		}
	}
	sort.Strings(names)
	return names, nil
}

func validateVisualArtifactSet(artifacts map[string][]byte) error {
	if len(artifacts) == 0 || len(artifacts) > visualMaxArtifactCount {
		return errors.New("invalid artifact count")
	}
	if _, ok := artifacts[visualManifestName]; !ok {
		return errors.New("artifact set has no manifest")
	}
	for name, payload := range artifacts {
		if _, err := canonicalVisualRelativePath(name, false); err != nil ||
			path.Base(name) != name ||
			strings.HasPrefix(name, ".") ||
			len(name) > visualMaxArtifactNameBytes {
			return errors.New("artifact set contains an invalid name")
		}
		if int64(len(payload)) > visualMaxArtifactBytes {
			return errors.New("artifact payload exceeds size bound")
		}
	}
	return nil
}

func checkVisualArtifactsInRoot(
	output *os.Root,
	artifacts map[string][]byte,
) ([]string, error) {
	entries, err := readBoundedVisualEntries(output, visualMaxOutputEntries)
	if err != nil {
		return nil, err
	}
	stale := make([]string, 0)
	present := make(map[string]struct{}, len(entries))
	for _, name := range entries {
		info, err := output.Lstat(name)
		if err != nil {
			return nil, errors.New("inspect artifact directory entry")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, errors.New("artifact directory contains a symlink")
		}
		_, managed := artifacts[name]
		switch {
		case info.IsDir():
			if managed {
				return nil, errors.New("managed artifact is a directory")
			}
			stale = append(stale, "unexpected:"+name)
		case !info.Mode().IsRegular():
			return nil, errors.New("artifact directory contains a special file")
		case !managed:
			stale = append(stale, "unexpected:"+name)
		default:
			present[name] = struct{}{}
		}
	}

	for _, name := range sortedArtifactNames(artifacts) {
		if _, ok := present[name]; !ok {
			stale = append(stale, name)
			continue
		}
		current, info, err := readBoundedVisualLeaf(
			output,
			name,
			visualMaxArtifactBytes,
		)
		if err != nil {
			return nil, errors.New("read managed artifact")
		}
		if info.Mode() != 0o644 || !bytes.Equal(current, artifacts[name]) {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	return stale, nil
}

func validateVisualOutputInventory(
	output *os.Root,
	artifacts map[string][]byte,
) error {
	entries, err := readBoundedVisualEntries(output, visualMaxOutputEntries)
	if err != nil {
		return err
	}
	for _, name := range entries {
		info, err := output.Lstat(name)
		if err != nil {
			return errors.New("inspect artifact directory entry")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("artifact directory contains a symlink")
		}
		if _, managed := artifacts[name]; !managed {
			return errors.New("artifact directory contains an unexpected entry")
		}
		if !info.Mode().IsRegular() {
			return errors.New("managed artifact is not a regular file")
		}
		if info.Size() < 0 || info.Size() > visualMaxArtifactBytes {
			return errors.New("managed artifact exceeds size bound")
		}
	}
	return nil
}

func writeVisualArtifactBundle(
	output *os.Root,
	artifacts map[string][]byte,
	writer visualArtifactWriter,
) error {
	if err := validateVisualArtifactSet(artifacts); err != nil {
		return err
	}
	if writer == nil {
		return errors.New("artifact writer is nil")
	}
	for _, name := range visualWriteOrder(artifacts) {
		if err := writer(output, name, artifacts[name]); err != nil {
			return err
		}
	}
	return nil
}

func atomicWriteVisual(output *os.Root, name string, payload []byte) error {
	return atomicWriteVisualWithHook(output, name, payload, nil)
}

func atomicWriteVisualWithHook(
	output *os.Root,
	name string,
	payload []byte,
	hook visualAtomicWriteHook,
) error {
	if _, err := canonicalVisualRelativePath(name, false); err != nil ||
		path.Base(name) != name {
		return errors.New("invalid artifact name")
	}
	if int64(len(payload)) > visualMaxArtifactBytes {
		return errors.New("artifact payload exceeds size bound")
	}
	if err := validateVisualReplaceTarget(output, name); err != nil {
		return err
	}

	temporary, file, err := createVisualTemporary(output, name)
	if err != nil {
		return err
	}
	remove := true
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		if remove {
			_ = output.Remove(temporary)
		}
	}()
	if err := runVisualAtomicWriteHook(hook, "after-create"); err != nil {
		return err
	}

	created, err := file.Stat()
	if err != nil || !created.Mode().IsRegular() {
		return errors.New("inspect temporary artifact")
	}
	if err := file.Chmod(0o644); err != nil {
		return errors.New("set artifact mode")
	}
	if err := writeAllVisual(file, payload); err != nil {
		return errors.New("write artifact")
	}
	if err := runVisualAtomicWriteHook(hook, "after-write"); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return errors.New("sync artifact")
	}
	if err := runVisualAtomicWriteHook(hook, "after-sync"); err != nil {
		return err
	}
	written, err := file.Stat()
	if err != nil ||
		!written.Mode().IsRegular() ||
		written.Mode() != 0o644 ||
		written.Size() != int64(len(payload)) ||
		!os.SameFile(created, written) {
		return errors.New("verify temporary artifact")
	}
	if err := file.Close(); err != nil {
		closed = true
		return errors.New("close artifact")
	}
	closed = true

	temporaryInfo, err := output.Lstat(temporary)
	if err != nil ||
		temporaryInfo.Mode()&os.ModeSymlink != 0 ||
		!temporaryInfo.Mode().IsRegular() ||
		temporaryInfo.Mode() != 0o644 ||
		!os.SameFile(written, temporaryInfo) {
		return errors.New("temporary artifact changed")
	}
	if err := validateVisualReplaceTarget(output, name); err != nil {
		return err
	}
	if err := runVisualAtomicWriteHook(hook, "before-rename"); err != nil {
		return err
	}
	if err := output.Rename(temporary, name); err != nil {
		return errors.New("replace artifact")
	}
	remove = false

	replaced, err := output.Lstat(name)
	if err != nil ||
		replaced.Mode()&os.ModeSymlink != 0 ||
		!replaced.Mode().IsRegular() ||
		replaced.Mode() != 0o644 ||
		replaced.Size() != int64(len(payload)) ||
		!os.SameFile(written, replaced) {
		return errors.New("verify replaced artifact")
	}
	if err := syncVisualDirectory(output); err != nil {
		return err
	}
	return nil
}

func runVisualAtomicWriteHook(
	hook visualAtomicWriteHook,
	stage string,
) error {
	if hook == nil {
		return nil
	}
	if err := hook(stage); err != nil {
		return errors.New("injected artifact write failure")
	}
	return nil
}

func createVisualTemporary(
	output *os.Root,
	name string,
) (string, *os.File, error) {
	var nonce [12]byte
	for range visualTemporaryAttempts {
		if _, err := rand.Read(nonce[:]); err != nil {
			return "", nil, errors.New("generate temporary artifact name")
		}
		temporary := "." + name + "." + hex.EncodeToString(nonce[:]) + ".tmp"
		file, err := output.OpenFile(
			temporary,
			os.O_RDWR|os.O_CREATE|os.O_EXCL,
			0o600,
		)
		if err == nil {
			return temporary, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, errors.New("create temporary artifact")
		}
	}
	return "", nil, errors.New("temporary artifact name attempts exhausted")
}

func validateVisualReplaceTarget(output *os.Root, name string) error {
	info, err := output.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return errors.New("inspect artifact target")
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("artifact target is not a real regular file")
	}
	return nil
}

func writeAllVisual(file *os.File, payload []byte) error {
	for len(payload) > 0 {
		written, err := file.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func syncVisualDirectory(root *os.Root) error {
	before, err := root.Stat(".")
	if err != nil || !before.IsDir() {
		return errors.New("inspect artifact directory")
	}
	directory, err := root.Open(".")
	if err != nil {
		return errors.New("open artifact directory")
	}
	opened, statErr := directory.Stat()
	if statErr != nil || !opened.IsDir() ||
		!sameVisualFileSnapshot(before, opened) {
		_ = directory.Close()
		return errors.New("artifact directory changed")
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return errors.New("sync artifact directory")
	}
	if closeErr != nil {
		return errors.New("close artifact directory")
	}
	return nil
}

func collectVisualSourceDirectory(
	root *os.Root,
	prefix string,
	includeTests bool,
	depth int,
	selection *visualSourceSelection,
) (resultErr error) {
	if depth > visualMaxSourceDepth {
		return errors.New("source tree exceeds depth bound")
	}
	directory, err := openVisualRelativeDirectory(root, prefix, false)
	if err != nil {
		return err
	}
	defer func() {
		if err := directory.Close(); resultErr == nil && err != nil {
			resultErr = errors.New("close source directory")
		}
	}()

	entries, err := readBoundedVisualEntries(
		directory,
		visualMaxSourceDirectoryEntries,
	)
	if err != nil {
		return err
	}
	if selection.entries > visualMaxSourceEntries-len(entries) {
		return errors.New("source tree exceeds entry bound")
	}
	selection.entries += len(entries)
	for _, name := range entries {
		info, err := directory.Lstat(name)
		if err != nil {
			return errors.New("inspect source tree entry")
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("source tree contains a symlink")
		}
		relative := path.Join(prefix, name)
		switch {
		case info.IsDir():
			if err := collectVisualSourceDirectory(
				root,
				relative,
				includeTests,
				depth+1,
				selection,
			); err != nil {
				return err
			}
		case !info.Mode().IsRegular():
			return errors.New("source tree contains a special file")
		case path.Ext(name) == ".go" &&
			(includeTests || !strings.HasSuffix(name, "_test.go")):
			selection.files[relative] = struct{}{}
		}
	}
	return nil
}

func sameVisualFileSnapshot(left, right fs.FileInfo) bool {
	return os.SameFile(left, right) &&
		left.Mode() == right.Mode() &&
		left.Size() == right.Size() &&
		left.ModTime().Equal(right.ModTime())
}
