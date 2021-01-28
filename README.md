# batch-reader
文件批量读取

# 文件批量读取
 1. 批量读取文件，主要用于大日志逐行度取，支持读取文本文件和GZ文件
 2. 支持文件读取行数记录，用于继续读取，
 
### 文件状态
```
name: /example/status.yaml
fileLine: {}
done:
- /example/data.txt

```

### 使用方法
```
package main

import (
	"context"
	"fmt"
	"github.com/itnxs/batch-reader"
	"github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
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
	r := batch_reader.NewFileBatchReader(2)
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
```

