package dbsync

import (
	"errors"
	"log"
	"os"
	"path/filepath"
)

// loadState reads a source's saved stream position. A missing file returns
// (nil, nil): the source starts from the current position.
func loadState(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// saveState writes the position atomically (temp file + rename), so a crash
// never leaves a half-written file.
func saveState(path string, data []byte) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func removeState(path string, logger *log.Logger) error {
	err := os.Remove(path)
	switch {
	case err == nil:
		logger.Printf("removed state file %s", path)
	case errors.Is(err, os.ErrNotExist):
		logger.Printf("state file %s doesn't exist", path)
	default:
		return err
	}
	return nil
}
