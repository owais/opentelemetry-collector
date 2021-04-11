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

package zkConfigSource

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-zookeeper/zk"
	"go.etcd.io/etcd/client"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/experimental/configsource"
)

var errInvalidPollInterval = errors.New("poll interval must be greater than zero")

// Error wrapper types to help with testability
type (
	errClientRead    struct{ error }
	errNilSecret     struct{ error }
	errNilSecretData struct{ error }
	errBadSelector   struct{ error }
)

type connectFunc func() (*zk.Conn, error)

type retrieved struct {
	value            interface{}
	watchForUpdateFn func() error
}

var _ configsource.Retrieved = (*retrieved)(nil)

func (r *retrieved) Value() interface{} {
	return r.value
}

func (r *retrieved) WatchForUpdate() error {
	return r.watchForUpdateFn()
}

// zkSession implements the configsource.Session interface.
type zkSession struct {
	logger        *zap.Logger
	connect       connectFunc
	cancelWatcher func()
}

var _ configsource.Session = (*zkSession)(nil)

func newSession(logger *zap.Logger, connect connectFunc) (*zkSession, error) {

	return &zkSession{
		logger:  logger,
		connect: connect,
	}, nil
}

func (s *zkSession) Retrieve(ctx context.Context, selector string, _ interface{}) (configsource.Retrieved, error) {
	resp, err := s.kapi.Get(ctx, selector, nil)
	if err != nil {
		return nil, err
	}

	watchCtx, cancel := context.WithCancel(context.Background())
	s.cancelWatcher = cancel

	return &retrieved{
		resp.Node.Value,
		s.newWatcher(watchCtx, selector, resp.Node.ModifiedIndex),
	}, nil
}

func (s *zkSession) RetrieveEnd(context.Context) error {
	return nil
}

func (s *zkSession) newWatcher(ctx context.Context, selector string, index uint64) func() error {
	return func() error {
		watcher := s.kapi.Watcher(selector, &client.WatcherOptions{AfterIndex: index})

		// exponential backoff with unlimited retries
		// unrecoverable cases handled inside loop
		ebo := backoff.NewExponentialBackOff()
		ebo.MaxElapsedTime = 0
		for {
			_, err := watcher.Next(ctx)
			if err == nil {
				return configsource.ErrValueUpdated
			}

			if err == context.Canceled {
				fmt.Println("watch cancelloed for key: ", selector)
				return configsource.ErrSessionClosed
			}

			if err == context.DeadlineExceeded {
				// TODO: should deadline exceeded return and trigger re-fetch?
				continue
			}
			// TODO: in case of unrecoverable errors, return from watch immediately

			// TODO: retry with exponential backoff for recoverable errors
			fmt.Println("error watching for key:: ", selector)
			select {
			case <-time.After(ebo.NextBackOff()):
				continue
			case <-ctx.Done():
				return configsource.ErrSessionClosed
			}

			// if key does not exist (e.g, was deleted), should we consider it as
			// an unrecoverable error as all invocations of watcher.Next in future will
			// continue to fail. May be we can add a large back-off in that case and wait
			// for the key to show up again
		}
	}
}

func (s *zkSession) Close(context.Context) error {
	if s.cancelWatcher != nil {
		s.cancelWatcher()
	}

	// todo: need to close client? clean up resources?
	return nil
}
