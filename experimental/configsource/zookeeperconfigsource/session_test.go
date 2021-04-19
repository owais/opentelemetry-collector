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
	"errors"
	"testing"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/experimental/configsource"
)

func sPtr(s string) *string {
	return &s
}

func TestSessionRetrieve(t *testing.T) {
	logger := zap.NewNop()
	conn := newMockConnection(map[string]string{
		"k1":       "v1",
		"d1/d2/k1": "v5",
	})
	connect := newMockConnectFunc(conn)

	session := &zkSession{logger: logger, connect: connect}

	testsCases := []struct {
		name   string
		key    string
		expect *string
		params interface{}
	}{
		{"present", "k1", sPtr("v1"), nil},
		{"present/params", "d1/d2/k1", sPtr("v5"), "param string"},
		{"absent", "k2", nil, nil},
	}

	for _, c := range testsCases {
		t.Run(c.name, func(t *testing.T) {
			retrieved, err := session.Retrieve(context.Background(), c.key, nil)
			if c.expect != nil {
				assert.NoError(t, err)
				assert.NotNil(t, retrieved.WatchForUpdate)
				return
			}
			assert.Error(t, err)
			assert.Nil(t, retrieved)
			assert.NoError(t, session.RetrieveEnd(context.Background()))
			assert.NoError(t, session.Close(context.Background()))
		})
	}
}

func TestWatcher(t *testing.T) {
	logger := zap.NewNop()
	conn := newMockConnection(map[string]string{
		"k1": "v1",
	})
	connect := newMockConnectFunc(conn)

	testsCases := []struct {
		name   string
		close  bool
		result string
		err    error
	}{
		{"updated", false, "v", nil},
		{"session-closed", true, "", nil},
		{"client-error", false, "", errors.New("client error")},
	}

	for _, c := range testsCases {
		t.Run(c.name, func(t *testing.T) {

			session := &zkSession{logger: logger, connect: connect}
			retrieved, err := session.Retrieve(context.Background(), "k1", nil)
			assert.NoError(t, err)
			assert.NotNil(t, retrieved.Value)
			assert.NotNil(t, retrieved.WatchForUpdate)

			go func() {
				watcher := conn.watches["k1"]
				switch {
				case c.close:
					session.Close(context.Background())
				case c.result != "":
					watcher <- zk.Event{
						Type: zk.EventNodeDataChanged,
					}
				case c.err != nil:
					watcher <- zk.Event{
						Err: err,
					}
				}
			}()

			err = retrieved.WatchForUpdate()
			switch {
			case c.close:
				assert.ErrorIs(t, err, configsource.ErrSessionClosed)
			case c.result != "":
				assert.ErrorIs(t, err, configsource.ErrValueUpdated)
			case c.err != nil:
				assert.ErrorIs(t, err, configsource.ErrSessionClosed)
			}
		})
	}
}
