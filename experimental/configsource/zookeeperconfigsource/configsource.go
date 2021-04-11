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

	"github.com/go-zookeeper/zk"
	"go.uber.org/zap"

	"go.opentelemetry.io/collector/experimental/configsource"
)

type zkConfigSource struct {
	logger    *zap.Logger
	conn      *zk.Conn
	endpoints []string
	timeout   uint
}

var _ configsource.ConfigSource = (*zkConfigSource)(nil)

func (v *zkConfigSource) NewSession(context.Context) (configsource.Session, error) {
	return newSession(v.logger, v.connect)
}

func (z *zkConfigSource) connect(context.Context) (*zk.Conn, error) {
	if z.conn != nil && z.conn.State() != zk.StateDisconnected {
		return z.conn, nil
	}

	conn, _, err := zk.Connect(z.endpoints, z.timeout, zk.WithLogInfo(false))
	if err != nil {
		return nil, err
	}
	z.conn = conn
	return z.conn, nil
}

func newConfigSource(logger *zap.Logger, cfg *Config) (*zkConfigSource, error) {
	z := &zkConfigSource{
		logger:    logger,
		endpoints: cfg.Endpoints,
		timeout:   cfg.Timeout,
	}

	conn, err := z.connect(context.Background())
	if err != nil {
		return nil, err
	}

	z.conn = conn
	return z, nil
}
