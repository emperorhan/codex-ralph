package ralph

import (
	"os"
	"testing"
)

func TestAutoFixPermissions(t *testing.T) {
	t.Parallel()

	paths := newTestPaths(t)
	if err := os.Chmod(paths.RalphDir, 0o700); err != nil {
		t.Fatalf("chmod ralph dir: %v", err)
	}
	if err := os.WriteFile(paths.ProfileLocalYAMLFile, []byte("plugin_name: universal-default\n"), 0o600); err != nil {
		t.Fatalf("write profile local yaml: %v", err)
	}

	result, err := AutoFixPermissions(paths)
	if err != nil {
		t.Fatalf("AutoFixPermissions failed: %v", err)
	}
	if len(result.UpdatedPaths) == 0 {
		t.Fatalf("expected updated paths")
	}

	info, err := os.Stat(paths.RalphDir)
	if err != nil {
		t.Fatalf("stat ralph dir: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("ralph dir mode mismatch: got=%#o want=%#o", got, 0o755)
	}

	fileInfo, err := os.Stat(paths.ProfileLocalYAMLFile)
	if err != nil {
		t.Fatalf("stat profile local yaml: %v", err)
	}
	if got := fileInfo.Mode().Perm(); got != 0o644 {
		t.Fatalf("profile local yaml mode mismatch: got=%#o want=%#o", got, 0o644)
	}
}
