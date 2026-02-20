package ralph

import (
	"os"
	"strings"
	"testing"
)

func TestDefaultServiceName(t *testing.T) {
	t.Parallel()

	got := DefaultServiceName("/tmp/My Project")
	if got != "ralph-my-project" {
		t.Fatalf("service name mismatch: got=%q want=%q", got, "ralph-my-project")
	}
}

func TestInstallServiceWithoutActivate(t *testing.T) {
	paths := newTestPaths(t)
	t.Setenv("HOME", t.TempDir())

	result, err := InstallService(paths, "/usr/local/bin/ralphctl", "", false)
	if err != nil {
		t.Fatalf("InstallService failed: %v", err)
	}
	if result.ServiceName == "" {
		t.Fatalf("service name should not be empty")
	}
	if result.UnitPath == "" {
		t.Fatalf("unit path should not be empty")
	}
	if result.Activated {
		t.Fatalf("activated should be false when activate=false")
	}
	content, err := os.ReadFile(result.UnitPath)
	if err != nil {
		t.Fatalf("read service file: %v", err)
	}
	if !strings.Contains(string(content), "supervise") {
		t.Fatalf("service file should run supervise command")
	}

	status, err := GetServiceStatus(paths, result.ServiceName)
	if err != nil {
		t.Fatalf("GetServiceStatus failed: %v", err)
	}
	if !status.Installed {
		t.Fatalf("service should be installed")
	}

	uninstalled, err := UninstallService(paths, result.ServiceName)
	if err != nil {
		t.Fatalf("UninstallService failed: %v", err)
	}
	if _, err := os.Stat(uninstalled.UnitPath); !os.IsNotExist(err) {
		t.Fatalf("service file should be removed")
	}
}
