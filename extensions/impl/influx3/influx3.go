// Copyright 2021-2025 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package influx3

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/tspoint"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

type influx3Client interface {
	GetServerVersion() (string, error)
	Close() error
	WritePoints(ctx context.Context, points []*influxdb3.Point, options ...influxdb3.WriteOption) error
}

type c struct {
	Host             string        `json:"host"`
	Token            string        `json:"token"`
	Database         string        `json:"database"`
	Measurement      string        `json:"measurement"`
	PrecisionStr     string        `json:"precision"`
	Precision        time.Duration `json:"-"`
	SSLRootsFilePath string        `json:"ssl_cert_path"`

	tspoint.WriteOptions
}

type influxSink3 struct {
	conf   c
	client influx3Client

	newClient func() (influx3Client, error)
}

func (m *influxSink3) Provision(ctx api.StreamContext, props map[string]any) error {
	m.conf = c{
		PrecisionStr: "ms",
		WriteOptions: tspoint.WriteOptions{
			PrecisionStr: "ms",
		},
	}

	if err := cast.MapToStruct(props, &m.conf); err != nil {
		return fmt.Errorf("error configuring influx3 sink: %s", err)
	}

	if len(m.conf.Host) == 0 {
		return fmt.Errorf("host is required")
	}

	if len(m.conf.Token) == 0 {
		return fmt.Errorf("token is required")
	}

	if len(m.conf.Database) == 0 {
		return fmt.Errorf("database is required")
	}

	if len(m.conf.Measurement) == 0 {
		return fmt.Errorf("measurement is required")
	}

	switch m.conf.PrecisionStr {
	case "s":
		m.conf.Precision = time.Second
	case "ms":
		m.conf.Precision = time.Millisecond
	case "us":
		m.conf.Precision = time.Microsecond
	case "ns":
		m.conf.Precision = time.Nanosecond
	default:
		return fmt.Errorf("precision %s is not supported", m.conf.PrecisionStr)
	}

	if err := cast.MapToStruct(props, &m.conf.WriteOptions); err != nil {
		return fmt.Errorf("error configuring influx3 sink: %s", err)
	}

	if err := m.conf.WriteOptions.Validate(); err != nil {
		return err
	}

	if m.newClient == nil {
		m.newClient = m.defaultNewClient
	}

	ctx.GetLogger().Infof("influx3 sink provision succesfully terminated")

	return nil
}

func (m *influxSink3) Consume(props map[string]any) {
	// Only swallow tags when they contain templates, otherwise keep them
	raw, ok := props["tags"]
	if !ok {
		return
	}

	tags, ok := raw.(map[string]any)
	if !ok {
		// if not map -> normal decoding error handling
		return
	}

	for _, v := range tags {
		s, ok := v.(string)
		if !ok {
			continue
		}
		// check if there are data template ({{ .value }}) -> dynamic properties
		if strings.Contains(s, "{{") && strings.Contains(s, "}}") {
			delete(props, "tags")
			return
		}
	}
}

func (m *influxSink3) transformPoints(ctx api.StreamContext, data any) ([]*influxdb3.Point, error) {
	rawPts, err := tspoint.SinkTransform(ctx, data, &m.conf.WriteOptions)
	if err != nil {
		ctx.GetLogger().Error(err)
		return nil, err
	}

	pts := make([]*influxdb3.Point, 0, len(rawPts))
	for _, rp := range rawPts {
		pts = append(pts, influxdb3.NewPoint(m.conf.Measurement, rp.Tags, rp.Fields, rp.Tt))
	}
	return pts, nil
}

func (m *influxSink3) defaultNewClient() (influx3Client, error) {
	config := influxdb3.ClientConfig{
		Host:             m.conf.Host,
		Token:            m.conf.Token,
		Database:         m.conf.Database,
		SSLRootsFilePath: m.conf.SSLRootsFilePath,
	}
	return influxdb3.New(config)
}

func (m *influxSink3) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnecting, "")
	if m.client == nil {
		client, err := m.newClient()
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
			return fmt.Errorf("Error creating influx3 client %s", err)
		}
		m.client = client
	}
	_, err := m.client.GetServerVersion()
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Influx3 sink connection error: %s", err)
	}
	sch(api.ConnectionConnected, "")
	ctx.GetLogger().Infof("influx3 succesfully connected")
	return nil
}

func (m *influxSink3) collect(ctx api.StreamContext, data any) error {
	if m.client == nil {
		return fmt.Errorf("influx3 sink: client not selected")
	}
	pts, err := m.transformPoints(ctx, data)
	if err != nil {
		return err
	}
	err = m.client.WritePoints(ctx, pts)
	if err != nil {
		return errorx.NewIOErr(fmt.Sprintf("influx3 sink failed to write data: %s", err))
	}
	ctx.GetLogger().Debug("successfully inserted data into influxdb3")
	return nil
}

func (m *influxSink3) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	return m.collect(ctx, item.ToMap())
}

func (m *influxSink3) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	return m.collect(ctx, items.ToMaps())
}

func (m *influxSink3) Close(ctx api.StreamContext) error {
	if m.client == nil {
		return fmt.Errorf("error closing client influx3: no client selected")
	}
	err := m.client.Close()
	if err != nil {
		return err
	}
	ctx.GetLogger().Infof("influx3 sink successfully closed")
	return nil
}

func (m *influxSink3) Ping(ctx api.StreamContext, props map[string]any) error {
	return m.Provision(ctx, props)
}

func (m *influxSink3) Info() model.SinkInfo {
	return model.SinkInfo{HasFields: true}
}

func GetSink() api.Sink {
	return &influxSink3{}
}

var (
	_ api.TupleCollector = &influxSink3{}
	_ util.PingableConn  = &influxSink3{}
	_ model.SinkInfoNode = &influxSink3{}
)
