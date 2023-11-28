package syncher

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/xattr"
	log "github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
)

const (
	overlayFSOpaqueXAttr string = "trusted.overlay.opaque"
)

type OverlayFSSyncher struct {
	lowerLayerPath string
	upperLayerPath string
	dryrun         bool
}

// NewOverlayFSSyncher creates a new OverlayFSSyncher
func NewOverlayFSSyncher(lower string, upper string) (*OverlayFSSyncher, error) {
	absLower, err := filepath.Abs(lower)
	if err != nil {
		return nil, xerrors.Errorf("failed to get abs lower path for %s: %w", lower, err)
	}

	absUpper, err := filepath.Abs(upper)
	if err != nil {
		return nil, xerrors.Errorf("failed to get abs upper path for %s: %w", upper, err)
	}

	return &OverlayFSSyncher{
		lowerLayerPath: absLower,
		upperLayerPath: absUpper,
	}, nil
}

// GetLowerLayerPath returns lower layer path
func (syncher *OverlayFSSyncher) GetLowerLayerPath() string {
	return syncher.lowerLayerPath
}

// GetUpperLayerPath returns upper layer path
func (syncher *OverlayFSSyncher) GetUpperLayerPath() string {
	return syncher.upperLayerPath
}

// SetDryRun sets dryrun
func (syncher *OverlayFSSyncher) SetDryRun(dryrun bool) {
	syncher.dryrun = dryrun
}

// Sync syncs upper layer data to lower layer
func (syncher *OverlayFSSyncher) Sync() error {
	walkFunc := func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return xerrors.Errorf("failed to walk %s: %w", path, err)
		}

		if d.IsDir() {
			if path == syncher.upperLayerPath {
				// skip root
				return nil
			}

			syncErr := syncher.syncDir(path)
			if syncErr != nil {
				return syncErr
			}
		} else {
			// file
			if d.Type()&os.ModeCharDevice != 0 {
				syncErr := syncher.syncWhiteout(path)
				if syncErr != nil {
					return syncErr
				}
			} else {
				syncErr := syncher.syncFile(path)
				if syncErr != nil {
					return syncErr
				}
			}
		}
		return nil
	}

	err := filepath.WalkDir(syncher.upperLayerPath, walkFunc)
	if err != nil {
		return xerrors.Errorf("failed to walk dir %s: %w", syncher.upperLayerPath, err)
	}

	return nil
}

func (syncher *OverlayFSSyncher) getLowerLayerPath(path string) (string, error) {
	relpath, err := filepath.Rel(syncher.upperLayerPath, path)
	if err != nil {
		return "", xerrors.Errorf("failed to get relative path from %s to %s", syncher.upperLayerPath, path)
	}

	lowerPath := filepath.Join(syncher.lowerLayerPath, relpath)

	return lowerPath, nil
}

func (syncher *OverlayFSSyncher) syncWhiteout(path string) error {
	logger := log.WithFields(log.Fields{
		"package":  "syncher",
		"function": "syncWhiteout",
	})

	logger.Debugf("processing whiteout file %s", path)

	lowerPath, err := syncher.getLowerLayerPath(path)
	if err != nil {
		return err
	}

	_, err = os.Stat(lowerPath)
	if err != nil {
		if os.IsNotExist(err) {
			// not exist
			logger.Debugf("file or dir %s not exist on lower", lowerPath)
			// suppress warning
			return nil
		}

		return xerrors.Errorf("failed to stat %s: %w", lowerPath, err)
	}

	logger.Debugf("deleting file or dir %s", lowerPath)

	if !syncher.dryrun {
		// remove
		err = os.RemoveAll(lowerPath)
		if err != nil {
			return xerrors.Errorf("failed to remove %s: %w", lowerPath, err)
		}
	}

	return nil
}

func (syncher *OverlayFSSyncher) syncFile(path string) error {
	logger := log.WithFields(log.Fields{
		"package":  "syncher",
		"function": "syncFile",
	})

	logger.Debugf("processing new or updated file %s", path)

	lowerPath, err := syncher.getLowerLayerPath(path)
	if err != nil {
		return err
	}

	lowerEntry, err := os.Stat(lowerPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return xerrors.Errorf("failed to stat %s: %w", lowerPath, err)
		}
	} else {
		// exist
		// if it is a dir, remove first
		// if it is a file, overwrite
		logger.Debugf("deleting dir %s", lowerPath)

		if !syncher.dryrun {
			if lowerEntry.IsDir() {
				// remove dir first
				err = os.RemoveAll(lowerPath)
				if err != nil {
					return xerrors.Errorf("failed to remove %s: %w", lowerPath, err)
				}
			}
		}
	}

	logger.Debugf("copying file %s", lowerPath)

	if !syncher.dryrun {
		// copy to lower layer
		src, err := os.Open(path)
		if err != nil {
			return xerrors.Errorf("failed to open %s: %w", path, err)
		}
		defer src.Close()

		dest, err := os.OpenFile(lowerPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return xerrors.Errorf("failed to open %s: %w", lowerPath, err)
		}
		defer dest.Close()

		_, err = io.Copy(dest, src)
		if err != nil {
			return xerrors.Errorf("failed to copy %s to %s: %w", path, lowerPath, err)
		}

		return nil
	}

	return nil
}

func (syncher *OverlayFSSyncher) syncDir(path string) error {
	logger := log.WithFields(log.Fields{
		"package":  "syncher",
		"function": "syncDir",
	})

	logger.Debugf("processing dir %s", path)

	lowerPath, err := syncher.getLowerLayerPath(path)
	if err != nil {
		return err
	}

	opaqueDir := false
	xattrVal, err := xattr.Get(path, overlayFSOpaqueXAttr)
	if err == nil {
		xattrValStr := string(xattrVal)
		logger.Debugf("xattr for path %s: %s = %s", path, overlayFSOpaqueXAttr, xattrValStr)

		if strings.ToLower(xattrValStr) == "y" {
			opaqueDir = true
		}
	}

	lowerEntry, err := os.Stat(lowerPath)
	if err != nil {
		if os.IsNotExist(err) {
			// not exist
			logger.Debugf("making dir %s", lowerPath)

			if !syncher.dryrun {
				err = os.MkdirAll(lowerPath, 0o700)
				if err != nil {
					return xerrors.Errorf("failed to make dir %s: %w", lowerPath, err)
				}
			}

			return nil
		}

		return xerrors.Errorf("failed to open %s: %w", path, err)
	}

	// exist
	// if it is a file, remove first
	// if it is a dir, merge or remove
	if !lowerEntry.IsDir() {
		// file
		logger.Debugf("deleting file %s", lowerPath)

		if !syncher.dryrun {
			err = os.RemoveAll(lowerPath)
			if err != nil {
				return xerrors.Errorf("failed to remove %s: %w", lowerPath, err)
			}
		}

		logger.Debugf("making dir %s", lowerPath)

		if !syncher.dryrun {
			err = os.MkdirAll(lowerPath, 0o700)
			if err != nil {
				return xerrors.Errorf("failed to make dir %s: %w", lowerPath, err)
			}
		}

		return nil
	}

	// merge or remove
	if opaqueDir {
		// remove
		logger.Debugf("emptying dir %s", lowerPath)

		if !syncher.dryrun {
			err = clearDirEntries(lowerPath)
			if err != nil {
				return xerrors.Errorf("failed to clear %s: %w", lowerPath, err)
			}
		}
	} else {
		// merge
		logger.Debugf("merging dir %s", lowerPath)
	}

	return nil
}

func clearDirEntries(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return xerrors.Errorf("failed to read dir %s", path)
	}

	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())

		err = os.RemoveAll(entryPath)
		if err != nil {
			return xerrors.Errorf("failed to remove %s", entryPath)
		}
	}

	return nil
}
