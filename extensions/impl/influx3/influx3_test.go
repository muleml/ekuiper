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
	"github.com/lf-edge/ekuiper/v2/pkg/model"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

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

func TestProvision_PrecisionVariants(t *testing.T) {
	ctx := mockContext.NewMockContext("testprecision", "op")

	tests := []struct {
		name      string
		precision string
		want      time.Duration
	}{
		{name: "seconds", precision: "s", want: time.Second},
		{name: "microseconds", precision: "us", want: time.Microsecond},
		{name: "nanoseconds", precision: "ns", want: time.Nanosecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &influxSink3{}

			props := map[string]any{
				"host":        "https://example:8086",
				"token":       "t",
				"database":    "db1",
				"measurement": "m",
				"precision":   tt.precision,
			}

			err := s.Provision(ctx, props)
			require.NoError(t, err)
			require.Equal(t, tt.precision, s.conf.PrecisionStr)
			require.Equal(t, tt.precision, s.conf.WriteOptions.PrecisionStr)
			require.Equal(t, tt.want, s.conf.Precision)
		})
	}
}

func TestConsume_EarlyReturnsAndKeepsTagsWhenNotTemplate(t *testing.T) {
	s := &influxSink3{}

	t.Run("no tags", func(t *testing.T) {
		props := map[string]any{"a": 1}
		s.Consume(props)
		require.Equal(t, map[string]any{"a": 1}, props)
	})

	t.Run("tags is not a map", func(t *testing.T) {
		props := map[string]any{"tags": "nope"}
		s.Consume(props)
		_, ok := props["tags"]
		require.True(t, ok)
	})

	t.Run("tags map has non-string values", func(t *testing.T) {
		props := map[string]any{
			"tags": map[string]any{
				"tag1": 123,
			},
		}
		s.Consume(props)
		_, ok := props["tags"]
		require.True(t, ok)
	})

	t.Run("tags map has string values without templates", func(t *testing.T) {
		props := map[string]any{
			"tags": map[string]any{
				"tag1": "value1",
				"tag2": "value2",
			},
		}
		s.Consume(props)
		_, ok := props["tags"]
		require.True(t, ok)
	})
}

func TestTransformPoints(t *testing.T) {
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

func TestTransformPointsError(t *testing.T) {
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

type testTuple struct{ m map[string]any }

func (t testTuple) Value(key, table string) (any, bool) { v, ok := t.m[key]; return v, ok }
func (t testTuple) ToMap() map[string]any               { return t.m }

type testTupleList struct{ ms []map[string]any }

func (l testTupleList) Len() int                 { return len(l.ms) }
func (l testTupleList) ToMaps() []map[string]any { return l.ms }
func (l testTupleList) RangeOfTuples(f func(index int, tuple api.MessageTuple) bool) {
	for i, m := range l.ms {
		if !f(i, testTuple{m: m}) {
			return
		}
	}
}

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
	if f.closeCalls > 1 {
		return f.closeErr
	}
	return nil
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

func baseConf() c {
	return c{
		Host:        "https://example:8086",
		Token:       "t",
		Database:    "db",
		Measurement: "m",
		WriteOptions: tspoint.WriteOptions{
			Tags: map[string]string{"tag": "v"},
		},
	}
}

func newConnectSink(cli func() (influx3Client, error)) *influxSink3 {
	return &influxSink3{
		conf:      baseConf(),
		newClient: cli,
	}
}

func newCollectSink(client influx3Client) *influxSink3 {
	return &influxSink3{
		conf:   baseConf(),
		client: client,
	}
}

func TestConnect_HappyPath(t *testing.T) {
	fc := &fakeInflux3Client{getVerRet: "3.0.0", getVerCalls: 0}

	newFakeClient := func() (influx3Client, error) {
		return fc, nil
	}
	ctx := mockContext.NewMockContext("connect_ok", "op")
	rec := &statusRecorder{}

	s := newConnectSink(newFakeClient)

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
	s := newConnectSink(newFakeClient)

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

	s := newConnectSink(newFakeClient)

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
	s := newConnectSink(newFakeClient)

	err := s.Connect(ctx, rec.handler)
	require.Error(t, err)
	require.Equal(t, 1, fc.getVerCalls)

	require.Len(t, rec.calls, 2)
	require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
	require.Equal(t, api.ConnectionDisconnected, rec.calls[1].status)
	require.Equal(t, rec.calls[1].message, "Connection error")
}

func TestInternalCollect_HappyPath(t *testing.T) {
	timex.Set(10)
	ctx := mockContext.NewMockContext("collect_ok", "op")

	fc := &fakeInflux3Client{}
	s := newCollectSink(fc)

	data := map[string]any{"t": 20}
	wantsPt := []*influxdb3.Point{
		influxdb3.NewPoint("m",
			map[string]string{"tag": "v"},
			map[string]any{"t": 20},
			time.UnixMilli(10),
		),
	}

	err := s.collect(ctx, data)
	require.NoError(t, err)

	require.Equal(t, 1, fc.writeCalls)
	require.Len(t, fc.lastPoints, 1)
	require.Equal(t, wantsPt[0].Values, fc.lastPoints[0].Values)
}

func TestInternalCollect_ReturnsErrorIfNotPresent(t *testing.T) {
	ctx := mockContext.NewMockContext("collect_not_present", "op")

	s := newCollectSink(nil)

	data := map[string]any{"temperature": 20}
	err := s.collect(ctx, data)
	require.ErrorContains(t, err, "client not selected")
}

func TestInternalCollect_PropagatesWritePointsErrorAsIOError(t *testing.T) {
	timex.Set(10)
	ctx := mockContext.NewMockContext("collect_write_err", "op")

	fc := &fakeInflux3Client{writeErr: errors.New("boom")}
	s := newCollectSink(fc)

	data := map[string]any{"temperature": 20}
	err := s.collect(ctx, data)
	require.Error(t, err)
	require.True(t, errorx.IsIOError(err))
	require.Contains(t, err.Error(), "boom")
}

func TestInternalCollect_TransformPointsError_DoesNotWrite(t *testing.T) {
	ctx := mockContext.NewMockContext("collect_transform_err", "op")

	fc := &fakeInflux3Client{}
	s := newCollectSink(fc)

	err := s.collect(ctx, []byte{1, 2, 3})
	require.Error(t, err)
	require.Equal(t, "sink needs map or []map, but receive unsupported data [1 2 3]", err.Error())
	require.Equal(t, 0, fc.writeCalls)
}

func TestCollect(t *testing.T) {
	timex.Set(10)
	ctx := mockContext.NewMockContext("collect_ok", "op")

	fc := &fakeInflux3Client{}
	s := newCollectSink(fc)

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

func TestCollectList(t *testing.T) {
	timex.Set(10)
	ctx := mockContext.NewMockContext("collect_ok", "op")

	fc := &fakeInflux3Client{}
	s := newCollectSink(fc)

	items := testTupleList{ms: []map[string]any{
		{"t": 20},
		{"t": 40},
	}}
	wantsPt := []*influxdb3.Point{
		influxdb3.NewPoint("m",
			map[string]string{"tag": "v"},
			map[string]any{"t": 20},
			time.UnixMilli(10),
		),
		influxdb3.NewPoint("m",
			map[string]string{"tag": "v"},
			map[string]any{"t": 40},
			time.UnixMilli(10),
		),
	}

	err := s.CollectList(ctx, items)
	require.NoError(t, err)

	require.Equal(t, 1, fc.writeCalls)
	require.Len(t, fc.lastPoints, 2)
	require.Equal(t, wantsPt[0].Values, fc.lastPoints[0].Values)
	require.Equal(t, wantsPt[1].Values, fc.lastPoints[1].Values)
}

func TestClose_ErrorWithNoClient(t *testing.T) {
	ctx := mockContext.NewMockContext("close_no_client", "op")

	s := &influxSink3{client: nil}

	err := s.Close(ctx)
	require.ErrorContains(t, err, "error closing client")
}

func TestClose_WithClient_CallsClientOnce(t *testing.T) {
	ctx := mockContext.NewMockContext("close_with_client", "op")

	fc := &fakeInflux3Client{}
	s := &influxSink3{client: fc}

	err := s.Close(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, fc.closeCalls)
}

func TestClose_ErrorIfClosedTwice(t *testing.T) {
	ctx := mockContext.NewMockContext("close_with_client", "op")

	fc := &fakeInflux3Client{closeErr: fmt.Errorf("error closing client")}
	s := &influxSink3{client: fc}

	err := s.Close(ctx)
	require.NoError(t, err)
	err = s.Close(ctx)
	require.Error(t, err)
	require.Equal(t, 2, fc.closeCalls)
}

func TestConsume_RemovesDynamicTagsFromPropsButKeepsInSink(t *testing.T) {
	ctx := mockContext.NewMockContext("consume_dynamic_tags", "op")

	props := map[string]any{
		"host":        "https://example:8086",
		"token":       "Token_test",
		"database":    "db1",
		"measurement": "test",
		"tags": map[string]any{
			"tag": "{{.value}}",
		},
		"fields":      []any{"temperature"},
		"tsFieldName": "ts",
	}

	s := &influxSink3{}
	require.NoError(t, s.Provision(ctx, props))

	_, ok := any(s).(model.PropsConsumer)
	require.True(t, ok, "influxSink3 must implement model.PropsConsumer")
	s.Consume(props)

	_, ok = props["tags"]
	require.False(t, ok, "Consume must remove tags from props if they contain templates")

	require.Equal(t, "{{.value}}", s.conf.WriteOptions.Tags["tag"], "sink must keep tag template for per row evaluation")
}

func TestPing_DelegatesToProvision(t *testing.T) {
	ctx := mockContext.NewMockContext("ping", "op")
	s := &influxSink3{}

	err := s.Ping(ctx, map[string]any{})
	require.Error(t, err)
	require.Equal(t, "host is required", err.Error())
}

func TestGetSink_ReturnsInfluxSink3(t *testing.T) {
	s := GetSink()
	require.NotNil(t, s)
	_, ok := s.(*influxSink3)
	require.True(t, ok)
}

func TestDefaultNewClient_CreatesClient(t *testing.T) {
	s := &influxSink3{
		conf: c{
			Host:             "https://example:8086",
			Token:            "t",
			Database:         "db",
			SSLRootsFilePath: "",
		},
	}

	cli, err := s.defaultNewClient()
	require.NoError(t, err)
	require.NotNil(t, cli)
}
