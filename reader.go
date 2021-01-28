package batch_reader

import (
	"context"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Handler 处理
// ctx 上下文
// data 行数据
type Handler func(ctx context.Context, data []byte) error

// FileBatchReader 文件批量读取
type FileBatchReader struct {
	wait sync.WaitGroup

	status  *status
	process int
	queue   chan fileLine
	handler Handler

	l *logrus.Logger
}

// NewFileBatchReader 新建一个文件批量读取
// process 携程数量
// statusName 状态存储地址
func NewFileBatchReader(process int) *FileBatchReader {
	return &FileBatchReader{
		status:  newStatus("./status.yaml"),
		process: process,
		queue:   make(chan fileLine, 100),
		l:       logrus.New(),
	}
}

// Run 运行
// ctx 上下文
// files 文件列表
// handler 处理函数
func (r *FileBatchReader) Run(files []string, handler Handler) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	go r.watch(cancel)

	r.handler = handler
	r.l.WithFields(logrus.Fields{"files": files}).Info("files")

	for p := 1; p <= r.process; p++ {
		r.wait.Add(1)
		go r.run(ctx)
	}

	for _, name := range files {
		if r.status.isFinish(name) {
			r.l.WithFields(logrus.Fields{"file": name}).Info("file is finish")
			continue
		}

		line := r.status.readLine(name)
		r.queue <- fileLine{Name: name, Line: line}
	}

	if len(r.queue) > 0 {
		r.wait.Wait()
	}

	return nil
}

func (r *FileBatchReader) watch(cancel context.CancelFunc) {
	sign := make(chan os.Signal, 1)
	signal.Notify(sign, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGSTOP)
	s := <-sign
	r.l.WithField("signal", s.String()).Info("receive signal")
	cancel()
}

func (r *FileBatchReader) run(ctx context.Context) {
	defer r.wait.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case file := <-r.queue:
			r.wait.Add(1)
			err := r.read(ctx, file)
			if err != nil {
				r.l.WithError(err).WithFields(logrus.Fields{"file": file}).Error("error")
				r.wait.Done()
				return
			}
			r.wait.Done()
			if len(r.queue) == 0 {
				return
			}
		}
	}
}

func (r *FileBatchReader) read(ctx context.Context, file fileLine) error {
	count := file.Line

	r.l.WithFields(logrus.Fields{
		"name": file.Name,
		"line": file.Line,
	}).Info("start read file")

	defer func() {
		err := r.status.save()
		if err != nil {
			r.l.WithError(err).WithField("status", r.status).Error("save status error")
		}
	}()

	var stats = make(map[string]int64)

	n := path.Base(file.Name)
	st := time.Now()
	err := ReadLine(ctx, file.Name, file.Line+1, func(data []byte) error {
		count++
		if count%10000 == 0 {
			et := time.Now()
			r.l.WithFields(logrus.Fields{
				"name":    n,
				"line":    count,
				"runtime": et.Sub(st).Seconds(),
			}).Infof("progress")
			st = et
		}

		err := r.handler(ctx, data)
		if err != nil {
			return errors.Wrap(err, "handle error")
		}

		stats["success"]++
		return nil
	})

	l := r.l.WithFields(logrus.Fields{
		"file":  file.Name,
		"line":  count,
		"stats": stats,
	})

	if err != nil {
		r.status.done(file.Name, count, false)
		if err == context.Canceled {
			l.Info("stop read file")
			return nil
		}
		return err
	}

	l.Info("end read file")
	r.status.done(file.Name, count, true)

	return nil
}
