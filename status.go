package batch_reader

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sync"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v2"
)

type status struct {
	mutex sync.RWMutex

	Name     string              `yaml:"name"`
	FileLine map[string]fileLine `yaml:"fileLine"`
	Done     []string            `yaml:"done"`
}

type fileLine struct {
	Name string `yaml:"file"`
	Line int64  `yaml:"line"`
}

func newStatus(name string) *status {
	name, err := filepath.Abs(name)
	if err != nil {
		panic(err)
	}
	s := &status{
		Name:     name,
		FileLine: map[string]fileLine{},
		Done:     []string{},
	}
	if err := s.load(); err != nil {
		panic(err)
	}
	return s
}

func (s *status) load() error {
	if !Exist(s.Name) {
		return nil
	}
	data, err := ioutil.ReadFile(s.Name)
	if err == nil {
		err = yaml.Unmarshal(data, &s)
	}
	return errors.WithStack(err)
}

func (s *status) save() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	data, err := yaml.Marshal(s)
	if err == nil {
		return ioutil.WriteFile(s.Name, data, 0644)
	}
	return errors.WithStack(err)
}

func (s *status) done(name string, line int64, isDone bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := fmt.Sprintf("%x", md5.Sum([]byte(name)))
	delete(s.FileLine, key)

	if isDone && line > 0 {
		s.Done = append(s.Done, name)
	} else if line > 0 {
		s.FileLine[key] = fileLine{Name: name, Line: line}
	}
}

func (s *status) readLine(name string) int64 {
	for _, v := range s.FileLine {
		if v.Name == name {
			return v.Line
		}
	}
	return 0
}

func (s *status) isFinish(name string) bool {
	for _, v := range s.Done {
		if v == name {
			return true
		}
	}
	return false
}
