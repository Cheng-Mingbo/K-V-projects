package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// DirSize 获取一个目录的大小
func DirSize(dirPath string) (int64, error) {
	var size int64
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() { // 如果不是目录，则累加文件大小
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// AvailableDiskSize 获取磁盘剩余可用空间大小
func AvailableDiskSize() (uint64, error) {
	wd, err := syscall.Getwd() // 获取当前工作目录
	if err != nil {
		return 0, err
	}
	var stat syscall.Statfs_t                        // 获取磁盘信息
	if err = syscall.Statfs(wd, &stat); err != nil { // 通过 syscall.Statfs 获取磁盘信息
		return 0, err
	}
	return stat.Bavail * uint64(stat.Bsize), nil // 返回磁盘剩余可用空间大小
}

// CopyDir 拷贝数据目录
func CopyDir(src, dest string, exclude []string) error {
	// 目标目标不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return err
		}
	}

	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		fileName := strings.Replace(path, src, "", 1) // 获取文件名
		if fileName == "" {
			return nil
		}

		for _, e := range exclude {
			matched, err := filepath.Match(e, info.Name()) // 判断是否匹配
			if err != nil {
				return err
			}
			if matched {
				return nil
			}
		}

		if info.IsDir() { // 如果是目录，则创建目录
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		// 如果是文件，则拷贝文件
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
