package ralph

import (
	"fmt"
	"os"
)

func IsEnabled(paths Paths) (bool, error) {
	m, err := ReadEnvFile(paths.StateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("read state file: %w", err)
	}
	v, ok := parseBool(m["RALPH_LOCAL_ENABLED"])
	if !ok {
		return true, nil
	}
	return v, nil
}

func SetEnabled(paths Paths, enabled bool) error {
	if err := EnsureLayout(paths); err != nil {
		return err
	}
	value := "false"
	if enabled {
		value = "true"
	}
	return os.WriteFile(paths.StateFile, []byte("RALPH_LOCAL_ENABLED="+value+"\n"), 0o644)
}
