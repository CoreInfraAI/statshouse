package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The opt-in --with-ui path builds the npm UI in a pinned node container and
// serves it from the api's --static-dir. apple/container has no in-container
// network, so the host first fills the npm cache with the container platform's
// packages and the container installs offline; docker installs online. The
// build is skipped while the source fingerprint and node image are unchanged.

const (
	// nodeBaseImage is the pinned Node toolchain for the UI build (node 20 as in
	// the UI CI). The digest is authoritative (multiarch index); the tag is for
	// humans.
	nodeBaseImage = "node:20.20.2-bookworm-slim@sha256:2cf067cfed83d5ea958367df9f966191a942351a2df77d6f0193e162b5febfc0"

	// npmMinMajor is the first npm with --os/--cpu/--libc, which populateNpmCache
	// needs.
	npmMinMajor = 7

	// In-container mount points for the UI build.
	uiSourceMount = "/ui-src"    // the statshouse-ui checkout (ro)
	uiOutputMount = "/ui-out"    // the cached build output (rw)
	npmCacheMount = "/npm-cache" // the host npm tarball cache (rw)

	// uiBuiltMarker is the uiBuildMarker file, kept outside build/ so wiping a
	// stale build/ never clears it.
	uiBuiltMarker = ".built"

	// npmCacheFingerprintFile records what the npm cache was last populated for
	// (see npmCacheFingerprint).
	npmCacheFingerprintFile = ".fingerprint"

	uiLibc = "glibc" // node:*-slim is Debian → glibc (not musl)
)

// uiFingerprintSkipDirs are non-source subtrees excluded from the source
// fingerprint.
var uiFingerprintSkipDirs = map[string]bool{"node_modules": true, "build": true, ".git": true}

// buildUI builds the npm UI (or reuses the cached build) and returns the host
// dir to mount as the api's --static-dir.
func buildUI(ctx context.Context, rt Runtime, repoRoot, cache, containerName string, log func(string, ...any)) (string, error) {
	uiDir := filepath.Join(repoRoot, "statshouse-ui")
	outRoot := filepath.Join(cache, "ui")
	outBuild := filepath.Join(outRoot, "build")
	markerPath := filepath.Join(outRoot, uiBuiltMarker)
	if err := os.MkdirAll(outRoot, 0o755); err != nil {
		return "", fmt.Errorf("create ui cache %s: %w", outRoot, err)
	}

	online := rt.HasNetworkEgress()

	fingerprint, err := uiSourceFingerprint(uiDir)
	if err != nil {
		return "", fmt.Errorf("fingerprint ui source: %w", err)
	}
	marker := readBuildMarker(markerPath)
	outMissing := !fileExists(filepath.Join(outBuild, "index.html"))
	if !uiNeedsRebuild(outMissing, marker, fingerprint, nodeBaseImage) {
		log("ui build: skipped (source fingerprint unchanged; cached build in %s)", outBuild)
		return outBuild, nil
	}

	// Mounted in both paths; only the offline path pre-populates it.
	npmCache := filepath.Join(cache, "npm")
	if err := os.MkdirAll(npmCache, 0o755); err != nil {
		return "", fmt.Errorf("create npm cache %s: %w", npmCache, err)
	}
	if online {
		log("ui build: docker runtime — npm ci runs online in the node container (NAT egress; no host cache populate)")
	} else {
		// cacache is additive: a re-populate after an arch switch just adds tarballs.
		fpPath := filepath.Join(npmCache, npmCacheFingerprintFile)
		wantFP, err := npmCacheFingerprint(uiDir)
		if err != nil {
			return "", fmt.Errorf("compute npm cache fingerprint: %w", err)
		}
		if readFileTrim(fpPath) != wantFP {
			if err := populateNpmCache(ctx, uiDir, npmCache, log); err != nil {
				return "", err
			}
			if err := os.WriteFile(fpPath, []byte(wantFP+"\n"), 0o644); err != nil {
				return "", fmt.Errorf("write npm cache fingerprint %s: %w", fpPath, err)
			}
		} else {
			log("ui build: npm cache up to date (%s)", npmCache)
		}
	}

	// A failed build leaves no output and the old marker, so the next run rebuilds.
	if err := os.RemoveAll(outBuild); err != nil {
		return "", fmt.Errorf("clear stale ui build output %s: %w", outBuild, err)
	}
	if err := os.MkdirAll(outBuild, 0o755); err != nil {
		return "", fmt.Errorf("recreate ui build output dir %s: %w", outBuild, err)
	}
	if err := buildUIInContainer(ctx, rt, uiDir, outBuild, npmCache, online, containerName, log); err != nil {
		return "", err
	}
	// buildUIInContainer verified index.html, so the build succeeded.
	if err := writeBuildMarker(markerPath, uiBuildMarker{Image: nodeBaseImage, Fingerprint: fingerprint}); err != nil {
		return "", fmt.Errorf("write ui build marker %s: %w", markerPath, err)
	}
	log("ui build: done (output in %s)", outBuild)
	return outBuild, nil
}

// populateNpmCache runs `npm install` on the host into a throwaway dir to fill
// npmCache with the linux/<arch>/glibc packages the offline container build
// needs (a plain host install would cache only darwin native deps).
// --ignore-scripts: postinstall scripts cannot run on the wrong OS, and the
// prebuilt binaries ship inside the tarballs.
func populateNpmCache(ctx context.Context, uiDir, npmCache string, log func(string, ...any)) error {
	work, err := os.MkdirTemp("", "statshouse-e2e-npmpop-*")
	if err != nil {
		return fmt.Errorf("create npm cache populate dir: %w", err)
	}
	defer os.RemoveAll(work)
	for _, f := range []string{"package.json", "package-lock.json"} {
		if err := copyFile(filepath.Join(uiDir, f), filepath.Join(work, f)); err != nil {
			return fmt.Errorf("stage %s for npm cache populate: %w", f, err)
		}
	}
	arch := containerNodeArch()
	args := []string{
		"install",
		"--os=linux",
		"--cpu=" + npmCPU(arch),
		"--libc=" + uiLibc,
		"--ignore-scripts",
		"--no-audit",
		"--no-fund",
		"--no-save",
		"--cache=" + npmCache,
	}
	start := time.Now()
	log("ui build: populating npm cache for linux/%s/%s on the host (one-time per lockfile/arch; host has internet, container does not)",
		npmCPU(arch), uiLibc)
	var b strings.Builder
	cmd := exec.CommandContext(ctx, "npm", args...)
	cmd.Dir = work
	cmd.Stdout = &b
	cmd.Stderr = &b
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return fmt.Errorf("npm install (populate cache): %w", ctx.Err())
		}
		return fmt.Errorf("npm install (populate cache): %w\n%s", err, indent(truncate(b.String(), 4000)))
	}
	log("ui build: npm cache populated (%.1fs)", time.Since(start).Seconds())
	return nil
}

// buildUIInContainer builds the UI in the node container, offline against the
// host-populated npm cache or online. The read-only source is copied to a
// container-local /work so the checkout stays clean; only build/ is copied out.
func buildUIInContainer(ctx context.Context, rt Runtime, uiDir, outBuild, npmCache string, online bool, containerName string, log func(string, ...any)) error {
	// Quirks: host node_modules/build are removed after the copy (hundreds of MB
	// of darwin binaries npm ci would wipe anyway). The copy-out uses `cp -r`, not
	// `cp -a`: on the virtiofs bind mount preserving attrs fails and GNU cp exits
	// non-zero. The chmod lets the non-root host user (lima) delete the root-owned
	// output next run; `|| true` because chmod may fail on virtiofs.
	npmCI := "npm ci --cache=" + npmCacheMount + " --no-audit --no-fund"
	mode := "offline"
	if online {
		npmCI += " --prefer-online"
		mode = "online"
	} else {
		npmCI += " --offline"
	}
	script := "set -e; " +
		"mkdir -p /work && cp -a " + uiSourceMount + "/. /work/ && " +
		"rm -rf /work/node_modules /work/build /work/.git && cd /work && " +
		"echo \"e2e: node=$(node -v) npm=$(npm -v)\" && " +
		npmCI + " && " +
		"echo \"e2e: npm ci ok (" + mode + ")\" && " +
		"npm run build && " +
		"mkdir -p " + uiOutputMount + " && cp -r /work/build/. " + uiOutputMount + "/ && " +
		"( chmod -R a+rwX " + uiOutputMount + " " + npmCacheMount + " 2>/dev/null || true ) && " +
		"echo \"e2e: ui build output copied to " + uiOutputMount + "\""
	opts := RunOpts{
		Name:  containerName,
		Image: nodeBaseImage,
		// No e2e network: online builds use the default bridge's NAT egress.
		Volumes: []string{
			uiDir + ":" + uiSourceMount + ":ro",
			outBuild + ":" + uiOutputMount,
			npmCache + ":" + npmCacheMount,
		},
		Cmd:    []string{"/bin/sh", "-c", script},
		AutoRm: true,
	}
	start := time.Now()
	log("ui build: running %s container (%s npm ci + npm run build)", nodeBaseImage, mode)
	res, err := run(ctx, rt.Name(), buildRunArgs(opts)...)
	output := res.stdout + res.stderr
	if err != nil {
		return fmt.Errorf("ui build container launch: %w\n%s", err, indent(truncate(output, 4000)))
	}
	if res.exitCode != 0 {
		return fmt.Errorf("ui build failed (container exit %d)\n%s", res.exitCode, indent(truncate(output, 6000)))
	}
	if !fileExists(filepath.Join(outBuild, "index.html")) {
		return fmt.Errorf("ui build produced no index.html in %s\n%s", outBuild, indent(truncate(output, 4000)))
	}
	log("ui build: container build ok (%.1fs)\n%s", time.Since(start).Seconds(), indent(truncate(strings.TrimSpace(output), 800)))
	return nil
}

// uiBuildMarker records the node image and source fingerprint of the last
// successful UI build.
type uiBuildMarker struct {
	Image       string `json:"image"`
	Fingerprint string `json:"fingerprint"`
}

// uiNeedsRebuild reports whether the cached UI build is stale. A zero marker
// always rebuilds.
func uiNeedsRebuild(outputMissing bool, marker uiBuildMarker, fingerprint, image string) bool {
	return outputMissing || marker.Image != image || marker.Fingerprint != fingerprint
}

// uiSourceFingerprint returns a sha256 over the sorted relative paths and
// contents of every source file in the UI tree. Content-based, unlike mtimes,
// so any edit, addition, deletion or rename changes it.
func uiSourceFingerprint(uiDir string) (string, error) {
	var paths []string
	walkErr := filepath.WalkDir(uiDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if uiFingerprintSkipDirs[d.Name()] {
				return filepath.SkipDir
			}
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if walkErr != nil {
		return "", walkErr
	}
	if len(paths) == 0 {
		return "", fmt.Errorf("no source files found under %s", uiDir)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, p := range paths {
		rel, err := filepath.Rel(uiDir, p)
		if err != nil {
			return "", fmt.Errorf("rel path %s: %w", p, err)
		}
		h.Write([]byte(filepath.ToSlash(rel)))
		h.Write([]byte{0})
		data, err := os.ReadFile(p)
		if err != nil {
			return "", fmt.Errorf("read %s for fingerprint: %w", p, err)
		}
		h.Write(data)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// uiIndexLooksBuilt reports whether body is the built UI's index.html (it has
// the React mount point) rather than the placeholder.
func uiIndexLooksBuilt(body string) bool {
	return strings.Contains(body, `id="root"`)
}

// assertUIServed polls GET / on the api until it serves the built UI and
// returns the body.
func assertUIServed(ctx context.Context, apiAddr string) (string, error) {
	url := "http://" + apiAddr + "/"
	const timeout = 60 * time.Second
	var (
		lastBody string
		lastCode int
	)
	if err := poll(ctx, timeout, 2*time.Second, func() (bool, error) {
		body, code, gerr := httpGet(ctx, url)
		if gerr != nil {
			lastBody = gerr.Error()
			return false, nil
		}
		lastCode, lastBody = code, body
		return code == http.StatusOK && uiIndexLooksBuilt(body), nil
	}); err != nil {
		return "", fmt.Errorf("UI not served at %s within %s (last code=%d, looks-built=%v): %v\nlast body: %s",
			url, timeout, lastCode, uiIndexLooksBuilt(lastBody), err, truncate(lastBody, 1000))
	}
	return lastBody, nil
}

// npmCacheFingerprint hashes the lockfiles, target platform and node image:
// the dep set the npm cache must hold.
func npmCacheFingerprint(uiDir string) (string, error) {
	return npmCacheFingerprintFor(uiDir, containerNodeArch(), uiLibc, nodeBaseImage)
}

// npmCacheFingerprintFor is npmCacheFingerprint with explicit inputs, for tests.
func npmCacheFingerprintFor(uiDir, arch, libc, image string) (string, error) {
	h := sha256.New()
	for _, f := range []string{"package.json", "package-lock.json"} {
		data, err := os.ReadFile(filepath.Join(uiDir, f))
		if err != nil {
			return "", fmt.Errorf("read %s for npm cache fingerprint: %w", f, err)
		}
		h.Write(data)
		h.Write([]byte{0})
	}
	fmt.Fprintf(h, "os=linux\ncpu=%s\nlibc=%s\nimage=%s\n", npmCPU(arch), libc, image)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// containerNodeArch is the node container's arch: the host's, NOT --arch (which
// only governs the Go cross-compile), since apple/container runs the node image
// at the host arch.
func containerNodeArch() string { return runtime.GOARCH }

// npmCPU maps a GOARCH to npm's --cpu value.
func npmCPU(arch string) string {
	switch arch {
	case "amd64":
		return "x64"
	case "386":
		return "ia32"
	default:
		return arch
	}
}

// npmMajorVersion returns the host npm's major version, or 0 if npm is missing
// or unparseable.
func npmMajorVersion(ctx context.Context) int {
	res, err := run(ctx, "npm", "--version")
	if err != nil || res.exitCode != 0 {
		return 0
	}
	major, _ := strconv.Atoi(strings.SplitN(strings.TrimSpace(res.stdout), ".", 2)[0])
	return major
}

// readFileTrim returns the file's trimmed contents, or "" if unreadable.
func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// readBuildMarker returns the build marker, or the zero marker (forcing a
// rebuild) when it is absent or unparseable.
func readBuildMarker(path string) uiBuildMarker {
	b, err := os.ReadFile(path)
	if err != nil || strings.TrimSpace(string(b)) == "" {
		return uiBuildMarker{}
	}
	var m uiBuildMarker
	if err := json.Unmarshal(b, &m); err != nil {
		return uiBuildMarker{}
	}
	return m
}

func writeBuildMarker(path string, m uiBuildMarker) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// copyFile copies a regular file, keeping its permissions.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	fi, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fi.Mode())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
