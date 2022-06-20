package batch_reader

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

// Exist 是否存在
// name 文件或则目录名称
func Exist(name string) bool {
	_, err := os.Stat(name)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

// CheckFiles 检查文件列表
// files 文件列表
func CheckFiles(files []string) error {
	for _, name := range files {
		if !Exist(name) {
			return fmt.Errorf("%s not exist", name)
		}
	}
	return nil
}

// LoadFiles 获取文件
// source 源目录名称
// all  是否获取所有子文件夹下的
func LoadFiles(source string, all ...bool) ([]string, error) {
	var (
		isReadAll bool
		files     = make([]string, 0)
	)

	if len(all) > 0 {
		isReadAll = all[0]
	}

	dir, err := filepath.Abs(source)
	if err != nil {
		return files, err
	}

	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return files, err
	}

	for _, f := range fs {
		fileName := f.Name()
		if fileName == "." || fileName == ".." || fileName == ".git" {
			continue
		}
		if f.IsDir() {
			if isReadAll {
				values, err := LoadFiles(path.Join(dir, fileName), isReadAll)
				if err != nil {
					return nil, err
				}
				files = append(files, values...)
			} else {
				continue
			}
		} else {
			files = append(files, path.Join(dir, fileName))
		}
	}

	return files, nil
}

// ReadLine 按行读取内容
// ctx 上下文
// source 文件地址
// line 开始如取的行号
// handle 读取后回调函数
func ReadLine(ctx context.Context, source string, line int64, handle func([]byte) error) error {
	file, err := os.Open(source)
	if err != nil {
		return err
	}

	defer file.Close()

	ext := path.Ext(source)
	if strings.EqualFold(ext, ".zst") {
		zst, err := zstd.NewReader(file)
		if err != nil {
			return errors.WithStack(err)
		}
		defer zst.Close()
		return read(ctx, zst, line, handle)
	} else if strings.EqualFold(ext, ".gz") {
		gz, err := gzip.NewReader(file)
		if err != nil {
			return errors.WithStack(err)
		}
		defer gz.Close()
		return read(ctx, gz, line, handle)
	}

	return read(ctx, file, line, handle)
}

func read(ctx context.Context, reader io.Reader, line int64, handle func([]byte) error) error {
	var (
		current int64
		err     error
		data    []byte
	)

	body := bufio.NewReader(reader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		current++
		data, err = body.ReadBytes('\n')
		data = bytes.Trim(data, "\n")
		if err == io.EOF {
			if len(data) > 0 {
				return handle(data)
			}
			return nil
		} else if err != nil {
			return err
		}

		if current < line {
			continue
		}

		err = handle(data)
		if err != nil {
			return err
		}
	}
}
