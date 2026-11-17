package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
)

func Write(path string, image Image) error {
	data, err := Encode(image)
	if err != nil {
		return err
	}
	if len(data) > MaxImage {
		return fmt.Errorf("checkpoint exceeds image limit")
	}
	return Atomic(path, data)
}
func Atomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".lamina-image-")
	if err != nil {
		return err
	}
	temp := f.Name()
	defer os.Remove(temp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
