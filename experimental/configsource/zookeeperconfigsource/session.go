// Copyright 2020 Splunk, Inc.
// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package zookeeperconfigsource

import (
	"context"

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/experimental/configsource"
)

// zkSession implements the configsource.Session interface.
type zkSession struct {
	logger      *zap.Logger
	connect     connectFunc
	cancelWatch func()
}

var _ configsource.Session = (*zkSession)(nil)

func (s *zkSession) Retrieve(ctx context.Context, selector string, _ interface{}) (configsource.Retrieved, error) {
	conn, err := s.connect(ctx)
	if err != nil {
		return nil, err
	}
	value, _, watchCh, err := conn.GetW(selector)
	if err != nil {
		return nil, err
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	s.cancelWatch = cancel

	return configsource.NewRetrieved(value, newWatcher(watchCtx, watchCh)), nil
}

func (s *zkSession) RetrieveEnd(context.Context) error {
	return nil
}

func (s *zkSession) Close(context.Context) error {
	if s.cancelWatch != nil {
		s.cancelWatch()
	}

	return nil
}

func newWatcher(ctx context.Context, watchCh <-chan zk.Event) func() error {
	return func() error {
		select {
		case <-ctx.Done():
			return configsource.ErrSessionClosed
		case e := <-watchCh:
			if e.Err != nil {
				return configsource.ErrSessionClosed
			}
			switch e.Type {
			case zk.EventNodeDataChanged, zk.EventNodeChildrenChanged:
				return configsource.ErrValueUpdated
			default:
				return configsource.ErrSessionClosed
			}
		}
	}
}
