package internal

import (
	"fmt"
	"runtime/debug"
	"strings"
)

const (
	defaultVersion = "0.0.0"
	defaultCommit  = "HEAD"
	defaultBuild   = "0000-01-01:00:00+00:00"
)

var (
	// Version is the tagged release version in the form <major>.<minor>.<patch>
	// following semantic versioning and is overwritten by the build system.
	Version = defaultVersion

	// Commit is the commit sha of the build (normally from Git) and is overwritten
	// by the build system.
	Commit = defaultCommit

	// Build is the date and time of the build as an RFC3339 formatted string
	// and is overwritten by the build system.
	Build = defaultBuild
)

// FullVersion display the full version and build
func FullVersion() string {
	var sb strings.Builder

	isDefault := Version == defaultVersion && Commit == defaultCommit && Build == defaultBuild

	if !isDefault {
		sb.WriteString(fmt.Sprintf("%s@%s %s", Version, Commit, Build))
	}

	if info, ok := debug.ReadBuildInfo(); ok {
		if isDefault {
			sb.WriteString(fmt.Sprintf(" %s", info.Main.Version))
		}
		sb.WriteString(fmt.Sprintf(" %s", info.GoVersion))
		if info.Main.Sum != "" {
			sb.WriteString(fmt.Sprintf(" %s", info.Main.Sum))
		}
	}

	return sb.String()
}
