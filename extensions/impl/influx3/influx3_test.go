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
	"testing"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/stretchr/testify/assert"

	"github.com/lf-edge/ekuiper/v2/extensions/impl/tspoint"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/timex"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     map[string]interface{}
		expected c
		error    string
	}{
		{
			name: "happy path",
			conf: map[string]interface{}{
				"host":        "https://example:8086",
				"token":       "Token_test",
				"database":    "db1",
				"measurement": "test",
				"tags": map[string]interface{}{
					"tag": "value",
				},
				"fields":      []interface{}{"temperature"},
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
			conf: map[string]interface{}{
				"database": 12,
			},
			error: "error configuring influx3 sink: 1 error(s) decoding:\n\n* 'database' expected type 'string', got unconvertible type 'int', value: '12'",
		},
		{
			name:  "host missing error",
			conf:  map[string]interface{}{},
			error: "host is required",
		},
		{
			name: "token missing error",
			conf: map[string]interface{}{
				"host": "https://example:8086",
			},
			error: "token is required",
		},
		{
			name: "database missing error",
			conf: map[string]interface{}{
				"host":  "https://example:8086",
				"token": "t",
			},
			error: "database is required",
		},
		{
			name: "measurement missing error",
			conf: map[string]interface{}{
				"host":     "https://example:8086",
				"token":    "t",
				"database": "db1",
			},
			error: "measurement is required",
		},
		{
			name: "precision invalid error",
			conf: map[string]interface{}{
				"host":        "https://example:8086",
				"token":       "t",
				"database":    "db1",
				"measurement": "m",
				"precision":   "abc",
			},
			error: "precision abc is not supported",
		},
		{
			name: "unmarshall error for tls",
			conf: map[string]interface{}{
				"host":        "https://example:8086",
				"token":       "t",
				"database":    "db1",
				"measurement": "mm",
				"rootCaPath":  12,
			},
			error: "error configuring tls: 1 error(s) decoding:\n\n* 'rootCaPath' expected type 'string', got unconvertible type 'int', value: '12'",
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

//
// func TestInfo(t *testing.T) {
// 	s := &influxSink3{}
// 	info := s.Info()
// 	assert.True(t, info.HasFields)
// }
//
// func TestCollectPointsError(t *testing.T) {
// 	tests := []struct {
// 		name string
// 		conf c
// 		data any
// 		err  string
// 	}{
// 		{
// 			name: "unsupported data",
// 			conf: c{
// 				Measurement: "test1",
// 				WriteOptions: tspoint.WriteOptions{
// 					Tags: map[string]string{
// 						"tag1": "value1",
// 						"tag2": "value2",
// 					},
// 				},
// 			},
// 			data: []byte{1, 2, 3},
// 			err:  "sink needs map or []map, but receive unsupported data [1 2 3]",
// 		},
// 		{
// 			name: "single without ts field",
// 			conf: c{
// 				Measurement: "test1",
// 				WriteOptions: tspoint.WriteOptions{
// 					Tags: map[string]string{
// 						"tag1": "value1",
// 						"tag2": "value2",
// 					},
// 					TsFieldName: "ts",
// 				},
// 			},
// 			data: map[string]any{
// 				"temperature": 20,
// 				"humidity":    50,
// 			},
// 			err: "time field ts not found",
// 		},
// 		{
// 			name: "normal batch with incorrect ts field",
// 			conf: c{
// 				Measurement: "test2",
// 				WriteOptions: tspoint.WriteOptions{
// 					Tags: map[string]string{
// 						"tag1": "value1",
// 						"tag2": "value2",
// 					},
// 					PrecisionStr: "s",
// 					TsFieldName:  "ts",
// 				},
// 			},
// 			data: []map[string]any{
// 				{
// 					"temperature": 20,
// 					"humidity":    50,
// 					"ts":          "add",
// 				},
// 				{
// 					"temperature": 30,
// 					"humidity":    60,
// 					"ts":          "ddd",
// 				},
// 			},
// 			err: "time field ts can not convert to timestamp(int64) : add",
// 		},
// 	}
//
// 	for _, test := range tests {
// 		t.Run(test.name, func(t *testing.T) {
// 			ifsink := &influxSink3{
// 				conf: test.conf,
// 			}
// 			ctx := mockContext.NewMockContext(test.name, "op")
// 			_, err := ifsink.transformPoints(ctx, test.data)
// 			assert.Error(t, err)
// 			assert.Equal(t, test.err, err.Error())
// 		})
// 	}
// }
//
// func TestCollectLines(t *testing.T) {
// 	// This test confirms line protocol generation by the influxdb3 Point itself.
// 	// We don't build line protocol in the sink; we only use MarshalBinary here for verification.
// 	timex.Set(10)
// 	ifsink := &influxSink3{conf: c{Measurement: "lp"}}
// 	ctx := mockContext.NewMockContext("lp", "op")
// 	points, err := ifsink.transformPoints(ctx, map[string]any{"name": "home"})
// 	assert.NoError(t, err)
// 	assert.Len(t, points, 1)
//
// 	lp, err := points[0].MarshalBinary(lineprotocol.Millisecond)
// 	assert.NoError(t, err)
// 	// String fields should be quoted in line protocol.
// 	assert.Equal(t, "lp name=\"home\" 10", string(lp))
// }
