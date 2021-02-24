package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/itnxs/batch-reader"
	"github.com/sirupsen/logrus"
)

func main() {
	// 读取目录文件
	files, err := batch_reader.LoadFiles("./")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go watch(cancel)

	// 读取文件内容
	r := batch_reader.NewFileBatchReader(5)
	err = r.Run(ctx, files, func(ctx context.Context, data []byte) error {
		fmt.Println(string(data))
		return nil
	})

	if err != nil {
		panic(err)
	}
}

func watch(cancel context.CancelFunc) {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGSTOP)
	s := <-sign
	logrus.WithField("signal", s.String()).Info("receive signal")
	cancel()
}
