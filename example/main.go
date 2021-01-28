package main

import (
	"context"
	"fmt"
	"github.com/itnxs/batch-reader"
)

func main() {
	// 读取目录文件
	files, err := batch_reader.LoadFiles("./")
	if err != nil {
		panic(err)
	}

	// 读取文件内容
	r := batch_reader.NewFileBatchReader(2)
	err = r.Run(files, func(ctx context.Context, data []byte) error {
		fmt.Println(string(data))
		return nil
	})

	if err != nil {
		panic(err)
	}
}
