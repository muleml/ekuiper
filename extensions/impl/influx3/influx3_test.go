// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/extensions/impl/tspoint"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

type testTuple struct{ m map[string]any }

func (t testTuple) Value(key, table string) (any, bool) { v, ok := t.m[key]; return v, ok }
func (t testTuple) ToMap() map[string]any               { return t.m }

type fakeInflux3Client struct {
	getVerCalls int
	getVerRet   string
	getVerErr   error

	createCalls int
	closeCalls  int
	closeErr    error

	writeCalls int
	gotCtx     context.Context
	lastPoints []*influxdb3.Point
	writeErr   error
}

func (f *fakeInflux3Client) WritePoints(ctx context.Context, pts []*influxdb3.Point, options ...influxdb3.WriteOption) error {
	f.writeCalls++
	f.gotCtx = ctx
	f.lastPoints = append([]*influxdb3.Point(nil), pts...)
	return f.writeErr
}

func (f *fakeInflux3Client) GetServerVersion() (string, error) {
	f.getVerCalls++
	return f.getVerRet, f.getVerErr
}

func (f *fakeInflux3Client) Close() error {
	f.closeCalls++
	return f.closeErr
}

type statusCall struct {
	status  string
	message string
}

type statusRecorder struct {
	calls []statusCall
}

func (r *statusRecorder) handler(status, message string) {
	r.calls = append(r.calls, statusCall{status: status, message: message})
}

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]any
		expected c
		error    string
	}{
		{
			name: "happy path",
			conf: map[string]any{
				"host":        "https://example:8086",
				"token":       "Token_test",
				"database":    "db1",
				"measurement": "test",
				"tags": map[string]any{
					"tag": "value",
				},
				"fields":      []any{"temperature"},
				"tsFieldName": "ts",
			},
			expected: c{
				Host:         "https://example:8086",
				Token:        "Token_test",
				Database:     "db1",
				Measurement:  "test",
				PrecisionStr: "ms",
				Precision:    time.Millisecond,
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag": "value",
					},
					TsFieldName:  "ts",
					PrecisionStr: "ms",
					Fields:       []string{"temperature"},
				},
			},
		},
		{
			name: "unmarshall error",
			conf: map[string]any{
				"database": 12,
			},
			error: "error configuring influx3 sink: 1 error(s) decoding:\n\n* 'database' expected type 'string', got unconvertible type 'int', value: '12'",
		},
		{
			name:  "host missing error",
			conf:  map[string]any{},
			error: "host is required",
		},
		{
			name: "token missing error",
			conf: map[string]any{
				"host": "https://example:8086",
			},
			error: "token is required",
		},
		{
			name: "database missing error",
			conf: map[string]any{
				"host":  "https://example:8086",
				"token": "t",
			},
			error: "database is required",
		},
		{
			name: "measurement missing error",
			conf: map[string]any{
				"host":     "https://example:8086",
				"token":    "t",
				"database": "db1",
			},
			error: "measurement is required",
		},
		{
			name: "precision invalid error",
			conf: map[string]any{
				"host":        "https://example:8086",
				"token":       "t",
				"database":    "db1",
				"measurement": "m",
				"precision":   "abc",
			},
			error: "precision abc is not supported",
		},
	}
	ctx := mockContext.NewMockContext("testconfig", "op")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink3{}
			err := ifsink.Provision(ctx, test.conf)
			if test.error == "" {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.Equal(t, test.error, err.Error())
				return
			}
			assert.Equal(t, test.expected, ifsink.conf)
		})
	}
}

func TestCollectPoints(t *testing.T) {
	timex.Set(10)
	tests := []struct {
		name string
		conf c
		data any
		prec lineprotocol.Precision
		want []*influxdb3.Point
	}{
		{
			name: "normal",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
				},
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			prec: lineprotocol.Millisecond,
			want: []*influxdb3.Point{
				influxdb3.NewPoint("test1",
					map[string]string{"tag1": "value1", "tag2": "value2"},
					map[string]any{"temperature": 20, "humidity": 50},
					time.UnixMilli(10),
				),
			},
		},
		{
			name: "normal batch",
			conf: c{
				Measurement: "test2",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					PrecisionStr: "s",
				},
				PrecisionStr: "s",
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
				},
				{
					"temperature": 30,
					"humidity":    60,
				},
			},
			prec: lineprotocol.Second,
			want: []*influxdb3.Point{
				influxdb3.NewPoint("test2",
					map[string]string{"tag1": "value1", "tag2": "value2"},
					map[string]any{"temperature": 20, "humidity": 50},
					time.UnixMilli(10),
				),
				influxdb3.NewPoint("test2",
					map[string]string{"tag1": "value1", "tag2": "value2"},
					map[string]any{"temperature": 30, "humidity": 60},
					time.UnixMilli(10),
				),
			},
		},
		{
			name: "normal batch with tag template and tsFieldName",
			conf: c{
				Measurement: "test3",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "{{.humidity}}",
						"tag2": "value2",
					},
					PrecisionStr: "s",
					TsFieldName:  "ts",
				},
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
					"ts":          100,
				},
				{
					"temperature": 30,
					"humidity":    60,
					"ts":          110,
				},
			},
			prec: lineprotocol.Second,
			want: []*influxdb3.Point{
				influxdb3.NewPoint("test3",
					map[string]string{"tag1": "50", "tag2": "value2"},
					map[string]any{"temperature": 20, "humidity": 50, "ts": 100},
					time.Unix(100, 0),
				),
				influxdb3.NewPoint("test3",
					map[string]string{"tag1": "60", "tag2": "value2"},
					map[string]any{"temperature": 30, "humidity": 60, "ts": 110},
					time.Unix(110, 0),
				),
			},
		},
		{
			name: "batch with us precision",
			conf: c{
				Measurement: "test4",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					PrecisionStr: "us",
					TsFieldName:  "ts",
				},
			},
			data: []map[string]any{
				{
					"t":  20,
					"ts": 100,
				},
				{
					"t":  30,
					"ts": 110,
				},
			},
			prec: lineprotocol.Microsecond,
			want: []*influxdb3.Point{
				influxdb3.NewPoint("test4",
					map[string]string{"tag1": "value1", "tag2": "value2"},
					map[string]any{"t": 20, "ts": 100},
					time.UnixMicro(100),
				),
				influxdb3.NewPoint("test4",
					map[string]string{"tag1": "value1", "tag2": "value2"},
					map[string]any{"t": 30, "ts": 110},
					time.UnixMicro(110),
				),
			},
		},
		{
			name: "single with ns precision",
			conf: c{
				Measurement: "test5",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "{{.humidity}}",
					},
					PrecisionStr: "ns",
					TsFieldName:  "ts",
				},
			},
			data: map[string]any{
				"humidity": 50,
				"ts":       100,
			},
			prec: lineprotocol.Nanosecond,
			want: []*influxdb3.Point{
				influxdb3.NewPoint("test5",
					map[string]string{"tag1": "value1", "tag2": "50"},
					map[string]any{"humidity": 50, "ts": 100},
					time.Unix(0, 100),
				),
			},
		},
		{
			name: "single with filtering fields",
			conf: c{
				Measurement: "test_fields",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
					},
					Fields:       []string{"t"},
					PrecisionStr: "ns",
				},
			},
			data: map[string]any{
				"t": 20,
				"h": 50,
			},
			prec: lineprotocol.Nanosecond,
			want: []*influxdb3.Point{
				influxdb3.NewPoint("test_fields",
					map[string]string{"tag1": "value1"},
					map[string]any{"t": 20},
					time.UnixMilli(10),
				),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink3{
				conf: test.conf,
			}
			ctx := mockContext.NewMockContext(test.name, "op")
			points, err := ifsink.transformPoints(ctx, test.data)
			assert.NoError(t, err)
			assert.Len(t, points, len(test.want))

			for i := range points {
				assert.NotNil(t, points[i])
				assert.Equal(t, test.want[i].Values, points[i].Values)
			}
		})
	}
}

func TestInfo(t *testing.T) {
	s := &influxSink3{}
	info := s.Info()
	assert.True(t, info.HasFields)
}

func TestCollectPointsError(t *testing.T) {
	tests := []struct {
		name string
		conf c
		data any
		err  string
	}{
		{
			name: "unsupported data",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
				},
			},
			data: []byte{1, 2, 3},
			err:  "sink needs map or []map, but receive unsupported data [1 2 3]",
		},
		{
			name: "single without ts field",
			conf: c{
				Measurement: "test1",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					TsFieldName: "ts",
				},
			},
			data: map[string]any{
				"temperature": 20,
				"humidity":    50,
			},
			err: "time field ts not found",
		},
		{
			name: "normal batch with incorrect ts field",
			conf: c{
				Measurement: "test2",
				WriteOptions: tspoint.WriteOptions{
					Tags: map[string]string{
						"tag1": "value1",
						"tag2": "value2",
					},
					PrecisionStr: "s",
					TsFieldName:  "ts",
				},
			},
			data: []map[string]any{
				{
					"temperature": 20,
					"humidity":    50,
					"ts":          "add",
				},
				{
					"temperature": 30,
					"humidity":    60,
					"ts":          "ddd",
				},
			},
			err: "time field ts can not convert to timestamp(int64) : add",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ifsink := &influxSink3{
				conf: test.conf,
			}
			ctx := mockContext.NewMockContext(test.name, "op")
			_, err := ifsink.transformPoints(ctx, test.data)
			assert.Error(t, err)
			assert.Equal(t, test.err, err.Error())
		})
	}
}

func TestConnect_HappyPath(t *testing.T) {
	fc := &fakeInflux3Client{getVerRet: "3.0.0", getVerCalls: 0}

	newFakeClient := func() (influx3Client, error) {
		return fc, nil
	}
	ctx := mockContext.NewMockContext("connect_ok", "op")
	rec := &statusRecorder{}

	s := &influxSink3{
		conf: c{
			Host:        "https://example:8086",
			Token:       "t",
			Database:    "db",
			Measurement: "m",
		},
		newClient: newFakeClient,
	}

	err := s.Connect(ctx, rec.handler)
	require.NoError(t, err)

	require.Equal(t, 1, fc.getVerCalls)
	require.Same(t, fc, s.client)

	require.Len(t, rec.calls, 2)
	require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
	require.Equal(t, api.ConnectionConnected, rec.calls[1].status)
}

func TestConnect_Idempotent_NoRecreate(t *testing.T) {
	ctx := mockContext.NewMockContext("connect_idem", "op")
	rec := &statusRecorder{}

	fc := &fakeInflux3Client{getVerRet: "3.0.0", getVerCalls: 0}
	newFakeClient := func() (influx3Client, error) {
		fc.createCalls++
		return fc, nil
	}
	s := &influxSink3{
		conf: c{
			Host:        "https://example:8086",
			Token:       "t",
			Database:    "db",
			Measurement: "m",
		},
		newClient: newFakeClient,
	}

	err := s.Connect(ctx, rec.handler)
	require.NoError(t, err)
	err = s.Connect(ctx, rec.handler)
	require.NoError(t, err)

	require.Equal(t, 1, fc.createCalls)
	require.Equal(t, 2, fc.getVerCalls)
	require.Same(t, fc, s.client)
}

func TestConnect_ConnectionError(t *testing.T) {
	fc := &fakeInflux3Client{getVerRet: "3.0.0", getVerCalls: 0}

	newFakeClient := func() (influx3Client, error) {
		return fc, fmt.Errorf("Error creating client")
	}
	ctx := mockContext.NewMockContext("connect_ok", "op")
	rec := &statusRecorder{}

	s := &influxSink3{
		conf: c{
			Host:        "https://example:8086",
			Token:       "t",
			Database:    "db",
			Measurement: "m",
		},
		newClient: newFakeClient,
	}

	err := s.Connect(ctx, rec.handler)
	require.Error(t, err)

	require.Equal(t, 0, fc.getVerCalls)

	require.Len(t, rec.calls, 2)
	require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
	require.Equal(t, api.ConnectionDisconnected, rec.calls[1].status)

	require.Nil(t, s.client)
}

func TestConnect_PingsError_DisconnectReturnsError(t *testing.T) {
	ctx := mockContext.NewMockContext("connect_error", "op")
	rec := &statusRecorder{}

	fc := &fakeInflux3Client{getVerRet: "3.0.0", getVerCalls: 0, getVerErr: fmt.Errorf("Connection error")}
	newFakeClient := func() (influx3Client, error) {
		return fc, nil
	}
	s := &influxSink3{
		conf: c{
			Host:        "https://example:8086",
			Token:       "t",
			Database:    "db",
			Measurement: "m",
		},
		newClient: newFakeClient,
	}

	err := s.Connect(ctx, rec.handler)
	require.Error(t, err)
	require.Equal(t, 1, fc.getVerCalls)

	require.Len(t, rec.calls, 2)
	require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
	require.Equal(t, api.ConnectionDisconnected, rec.calls[1].status)
	require.Equal(t, rec.calls[1].message, "Connection error")
}

func TestCollect_CallsWritePointsOnceWithTransformedPoints(t *testing.T) {
	timex.Set(10)
	ctx := mockContext.NewMockContext("collect_ok", "op")

	s := &influxSink3{
		conf: c{
			Measurement: "m",
			WriteOptions: tspoint.WriteOptions{
				Tags: map[string]string{"tag": "v"},
			},
		},
	}
	fc := &fakeInflux3Client{}
	s.client = fc

	item := testTuple{m: map[string]any{"t": 20}}
	wantsPt := []*influxdb3.Point{
		influxdb3.NewPoint("m",
			map[string]string{"tag": "v"},
			map[string]any{"t": 20},
			time.UnixMilli(10),
		),
	}

	err := s.Collect(ctx, item)
	require.NoError(t, err)

	require.Equal(t, 1, fc.writeCalls)
	require.Len(t, fc.lastPoints, 1)
	require.Equal(t, wantsPt[0].Values, fc.lastPoints[0].Values)
}

func TestCollect_ReturnsErrorIfNotPresent(t *testing.T) {
	ctx := mockContext.NewMockContext("collect_not_present", "op")

	s := &influxSink3{
		conf: c{
			Measurement: "m",
			WriteOptions: tspoint.WriteOptions{
				Tags: map[string]string{"tag": "v"},
			},
		},
		client: nil,
	}

	err := s.Collect(ctx, testTuple{m: map[string]any{"temperature": 20}})
	require.ErrorContains(t, err, "client not selected")
}

func TestCollect_PropagatesWritePointsErrorAsIOError(t *testing.T) {
	timex.Set(10)
	ctx := mockContext.NewMockContext("collect_write_err", "op")

	s := &influxSink3{
		conf: c{
			Measurement: "m",
			WriteOptions: tspoint.WriteOptions{
				Tags: map[string]string{"tag": "v"},
			},
		},
	}
	fc := &fakeInflux3Client{writeErr: errors.New("boom")}
	s.client = fc

	err := s.Collect(ctx, testTuple{m: map[string]any{"temperature": 20}})
	require.Error(t, err)
	require.True(t, errorx.IsIOError(err))
	require.Contains(t, err.Error(), "boom")
}
