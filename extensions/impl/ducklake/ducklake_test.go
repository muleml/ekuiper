package ducklake

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/stretchr/testify/require"
)

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

type fakeResult struct {
	lastInsertID int64
	rowsAffected int64
}

func (r fakeResult) LastInsertId() (int64, error) { return r.lastInsertID, nil }
func (r fakeResult) RowsAffected() (int64, error) { return r.rowsAffected, nil }

type fakeConn struct {
	queries           []string
	errStr            string
	numCorrectQueries int
	closeCalls        int
	closeErr          string
}

func (f *fakeConn) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	f.queries = append(f.queries, query)
	if f.errStr != "" && len(f.queries) >= f.numCorrectQueries {
		return nil, fmt.Errorf("%s", f.errStr)
	}
	return fakeResult{}, nil
}

func (f *fakeConn) Close() error {
	f.closeCalls++
	if f.closeErr != "" {
		return fmt.Errorf("%s", f.closeErr)
	}
	return nil
}

type fakeDB struct {
	closeCalls int
	closeErr   string
	connCalls  int
	connErr    string
	mockConn   *sql.Conn
}

func (f *fakeDB) Conn(ctx context.Context) (*sql.Conn, error) {
	f.connCalls++
	if f.connErr != "" {
		return nil, fmt.Errorf("%s", f.connErr)
	}
	return f.mockConn, nil
}

func (f *fakeDB) Close() error {
	f.closeCalls++
	if f.closeErr != "" {
		return fmt.Errorf("%s", f.closeErr)
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

type fakeArrowViewManager struct {
	calls       int
	lastName    string
	released    bool
	registerErr error
}

func (f *fakeArrowViewManager) RegisterRecordBatch(name string, batch arrow.RecordBatch) (func(), error) {
	f.calls++
	f.lastName = name
	if f.registerErr != nil {
		return nil, f.registerErr
	}
	return func() {
		f.released = true
	}, nil
}

type fakeLogger struct {
	errorf []string
	warnf  []string
}

func (l *fakeLogger) Debug(args ...any)     {}
func (l *fakeLogger) Debugf(string, ...any) {}
func (l *fakeLogger) Info(args ...any)      {}
func (l *fakeLogger) Infof(string, ...any)  {}
func (l *fakeLogger) Warn(args ...any)      {}
func (l *fakeLogger) Error(args ...any)     {}
func (l *fakeLogger) Warnf(format string, args ...any) {
	l.warnf = append(l.warnf, fmt.Sprintf(format, args...))
}
func (l *fakeLogger) Errorf(format string, args ...any) {
	l.errorf = append(l.errorf, fmt.Sprintf(format, args...))
}

type loggerOverrideCtx struct {
	api.StreamContext
	l api.Logger
}

func (c *loggerOverrideCtx) GetLogger() api.Logger { return c.l }

func newMockCtxWithFakeLogger(rule, op string) (api.StreamContext, *fakeLogger) {
	base := mockContext.NewMockContext(rule, op)
	fl := &fakeLogger{}
	return &loggerOverrideCtx{StreamContext: base, l: fl}, fl
}

func TestProvision_Config(t *testing.T) {
	ctx := mockContext.NewMockContext("testprovision", "op")

	tests := []struct {
		name     string
		conf     map[string]any
		expected c
		errStr   string
	}{
		{
			name: "defaults (duckb catalog)",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
			},
			expected: c{
				Catalog: CatalogConf{
					Type: "duckdb",
				},
				Storage: StorageConf{
					Type:     "s3",
					Endpoint: "test-endpoint:9000",
					Bucket:   "ducklake",
					KeyId:    "test_id",
					Secret:   "test_secret",
				},
				Table:          "table",
				sanitizedTable: "table",
				ducklakeName:   "the_ducklake",
			},
		},
		{
			name: "bucket name not ducklake",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "not-ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
			},
			expected: c{
				Catalog: CatalogConf{
					Type: "duckdb",
				},
				Storage: StorageConf{
					Type:     "s3",
					Endpoint: "test-endpoint:9000",
					Bucket:   "not-ducklake",
					KeyId:    "test_id",
					Secret:   "test_secret",
				},
				Table:          "table",
				sanitizedTable: "table",
				ducklakeName:   "the_ducklake",
			},
		},
		{
			name: "duckb catalog",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
			},
			expected: c{
				Catalog: CatalogConf{
					Type: "duckdb",
				},
				Storage: StorageConf{
					Type:     "s3",
					Endpoint: "test-endpoint:9000",
					Bucket:   "ducklake",
					KeyId:    "test_id",
					Secret:   "test_secret",
				},
				Table:          "table",
				sanitizedTable: "table",
				ducklakeName:   "the_ducklake",
			},
		},
		{
			name: "postgres catalog",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
			},
			expected: c{
				Catalog: CatalogConf{
					Type:     "postgres",
					Host:     "postgres",
					Port:     5432,
					Database: "ducklake_catalog",
					User:     "user",
					Password: "password",
				},
				Storage: StorageConf{
					Type:     "s3",
					Endpoint: "test-endpoint:9000",
					Bucket:   "ducklake",
					KeyId:    "test_id",
					Secret:   "test_secret",
				},
				Table:          "table",
				sanitizedTable: "table",
				ducklakeName:   "the_ducklake",
			},
		},
		{
			name: "unmarshal error",
			conf: map[string]any{
				"catalog": 12,
			},
			errStr: "error configuring ducklake sink",
		},
		{
			name: "storage missing error",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: missing storage",
		},
		{
			name: "table missing error",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
			},
			errStr: "error configuring ducklake sink: missing table name",
		},
		{
			name: "postgres missing host error",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_database": "ducklake_catalog",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: host is required for postgres",
		},
		{
			name: "postgres missing database name error",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type": "postgres",
					"catalog_host": "postgres",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: database name is required for postgres",
		},
		{
			name: "catalog not supported",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type": "mysql",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: catalog not supported",
		},
		{
			name: "catalog not supported",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type": "mysql",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: catalog not supported",
		},
		{
			name: "storage missing error",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: missing storage",
		},
		{
			name: "storage s3 missing endpoint",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":   "s3",
					"storage_bucket": "ducklake",
					"storage_key_id": "test_id",
					"storage_secret": "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: missing storage s3 endpoint",
		},
		{
			name: "storage s3 missing bucket",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: missing storage s3 bucket",
		},
		{
			name: "storage type not supported",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "cloud",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_secret":   "test_secret",
				},
				"table": "table",
			},
			errStr: "error configuring ducklake sink: storage type not supported",
		},
		{
			name: "sanitized table name",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
				},
				"table":        `my table "v1"`,
				"ducklakeName": "the_ducklake",
			},
			expected: c{
				Catalog: CatalogConf{Type: "duckdb"},
				Storage: StorageConf{
					Type:     "s3",
					Endpoint: "test-endpoint:9000",
					Bucket:   "ducklake",
				},
				Table:          `my table "v1"`,
				sanitizedTable: `mytablev1`,
				ducklakeName:   "the_ducklake",
			},
		},
		{
			name: "invalid table name: ;",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
				},
				"table": `; DROP TABLES`,
			},
			errStr: "error configuring ducklake sink: invalid table name",
		},
		{
			name: "invalid table name: newline",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
				},
				"table": "t\tx",
			},
			errStr: "error configuring ducklake sink: invalid table name",
		},
		{
			name: "fields from props",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
				"fields":       []any{"a", "b"},
			},
			expected: c{
				Catalog: CatalogConf{
					Type: "duckdb",
				},
				Storage: StorageConf{
					Type:     "s3",
					Endpoint: "test-endpoint:9000",
					Bucket:   "ducklake",
					KeyId:    "test_id",
					Secret:   "test_secret",
				},
				Table:          "table",
				sanitizedTable: "table",
				ducklakeName:   "the_ducklake",
				Fields:         []string{"a", "b"},
			},
		},
		{
			name: "duplicate field",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
				"fields":       []any{"a", "a"},
			},
			errStr: "duplicated field",
		},
		{
			name: "empty field",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table":        "table",
				"ducklakeName": "the_ducklake",
				"fields":       []any{"a", ""},
			},
			errStr: "empty field",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &DuckLakeSink{}
			err := s.Provision(ctx, tt.conf)
			if tt.errStr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errStr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, s.conf)
		})
	}
}

func TestConnect(t *testing.T) {
	ctx := mockContext.NewMockContext("testconnect", "op")

	tests := []struct {
		name              string
		conf              map[string]any
		expected          []string
		errStr            string
		numCorrectQueries int
		useFakeConn       bool
	}{
		{
			name: "default",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			useFakeConn: true,
		},
		{
			name: "default bucket not ducklake",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "not-ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://not-ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			useFakeConn: true,
		},
		{
			name: "happy path - ssl url style",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"use_ssl":          true,
					"url_style":        "vhost",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL TRUE, URL_STYLE 'vhost');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
		},
		{
			name: "duckb catalog",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			useFakeConn: true,
		},
		{
			name: "postgres catalog",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			useFakeConn: true,
		},
		{
			name: "error install ducklake",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 1,
			useFakeConn:       true,
		},
		{
			name: "error install postgres",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 2,
			useFakeConn:       true,
		},
		{
			name: "error storage secret",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 3,
			useFakeConn:       true,
		},
		{
			name: "error catalog secret",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 4,
			useFakeConn:       true,
		},
		{
			name: "error ducklake secret",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 5,
			useFakeConn:       true,
		},
		{
			name: "error attach ducklake",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 6,
			useFakeConn:       true,
		},
		{
			name: "check arrow manager is set",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			useFakeConn: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &statusRecorder{}
			if tt.useFakeConn {
				fconn := &fakeConn{errStr: tt.errStr, numCorrectQueries: tt.numCorrectQueries}
				s := &DuckLakeSink{conn: fconn}
				err := s.Provision(ctx, tt.conf)
				require.NoError(t, err)
				err = s.Connect(ctx, rec.handler)
				if tt.errStr != "" {
					require.Error(t, err)
					require.ErrorContains(t, err, tt.errStr)
					require.Len(t, rec.calls, 2)
					require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
					require.Equal(t, api.ConnectionDisconnected, rec.calls[1].status)
					require.Equal(t, tt.expected, fconn.queries)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tt.expected, fconn.queries)
				require.Len(t, rec.calls, 2)
				require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
				require.Equal(t, api.ConnectionConnected, rec.calls[1].status)
			} else {
				s := &DuckLakeSink{}
				err := s.Provision(ctx, tt.conf)
				require.NoError(t, err)
				err = s.Connect(ctx, rec.handler)
				require.NoError(t, err)
				require.NotNil(t, s.arrowMgr)
			}
		})
	}
}

func TestClose(t *testing.T) {
	ctx := mockContext.NewMockContext("testclose", "op")

	t.Run("no db selected", func(t *testing.T) {
		s := &DuckLakeSink{}
		err := s.Close(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "error closing ducklake sink")
		require.ErrorContains(t, err, "no db to close")
	})

	t.Run("no conn selected", func(t *testing.T) {
		fakedb := &fakeDB{}
		s := &DuckLakeSink{db: fakedb}
		err := s.Close(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "error closing ducklake sink")
		require.ErrorContains(t, err, "no conn to close")
	})

	t.Run("conn is not closable", func(t *testing.T) {
		fakeconn := &fakeConn{closeErr: "conn is not closable"}
		fakedb := &fakeDB{}
		s := &DuckLakeSink{db: fakedb, conn: fakeconn}
		err := s.Close(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "error closing ducklake sink")
		require.ErrorContains(t, err, "conn is not closable")
		require.Equal(t, 1, fakeconn.closeCalls)
	})

	t.Run("db is not closable", func(t *testing.T) {
		fakeconn := &fakeConn{}
		fakedb := &fakeDB{closeErr: "db is not closable"}
		s := &DuckLakeSink{db: fakedb, conn: fakeconn}
		err := s.Close(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "error closing ducklake sink")
		require.ErrorContains(t, err, "db is not closable")
		require.Equal(t, 1, fakedb.closeCalls)
	})

	t.Run("close ok", func(t *testing.T) {
		db, _, _ := sqlmock.New()
		t.Cleanup(func() { _ = db.Close() })
		conn, _ := db.Conn(ctx)
		t.Cleanup(func() { _ = conn.Close() })
		fakedb := &fakeDB{mockConn: conn}
		fakeconn := &fakeConn{}
		s := &DuckLakeSink{
			db:   fakedb,
			conn: fakeconn,
		}
		err := s.Close(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, fakeconn.closeCalls)
		require.Equal(t, 1, fakedb.closeCalls)
	})
}

func TestPing(t *testing.T) {
	ctx := mockContext.NewMockContext("testconnect", "op")

	tests := []struct {
		name              string
		conf              map[string]any
		expected          []string
		errStr            string
		numCorrectQueries int
	}{
		{
			name: "happy path",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE memory;",
				"DETACH the_ducklake;",
			},
		},
		{
			name: "happy path - ssl url style",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"use_ssl":          true,
					"url_style":        "vhost",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL TRUE, URL_STYLE 'vhost');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE memory;",
				"DETACH the_ducklake;",
			},
		},
		{
			name: "duckb catalog",
			conf: map[string]any{
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE memory;",
				"DETACH the_ducklake;",
			},
		},
		{
			name: "postgres catalog",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE memory;",
				"DETACH the_ducklake;",
			},
		},
		{
			name: "error install ducklake",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 1,
		},
		{
			name: "error install postgres",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 2,
		},
		{
			name: "error storage secret",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 3,
		},
		{
			name: "error catalog secret",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 4,
		},
		{
			name: "error ducklake secret",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 5,
		},
		{
			name: "error attach ducklake",
			conf: map[string]any{
				"catalog": map[string]any{
					"catalog_type":     "postgres",
					"catalog_host":     "postgres",
					"catalog_port":     5432,
					"catalog_database": "ducklake_catalog",
					"catalog_user":     "user",
					"catalog_password": "password",
				},
				"storage": map[string]any{
					"storage_type":     "s3",
					"storage_endpoint": "test-endpoint:9000",
					"storage_bucket":   "ducklake",
					"storage_key_id":   "test_id",
					"storage_secret":   "test_secret",
					"url_style":        "path",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000', USE_SSL FALSE, URL_STYLE 'path');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := &fakeConn{errStr: tt.errStr, numCorrectQueries: tt.numCorrectQueries}
			db := &fakeDB{}
			s := &DuckLakeSink{db: db, conn: conn}
			err := s.Ping(ctx, tt.conf)
			require.Equal(t, 1, db.closeCalls)
			require.Equal(t, 1, conn.closeCalls)
			if tt.errStr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errStr)
				require.Equal(t, tt.expected, conn.queries)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, conn.queries)
		})
	}
}

func TestBuildArrowData(t *testing.T) {
	ctx, fl := newMockCtxWithFakeLogger("collect", "op")

	ts, _ := time.Parse(time.RFC3339, "2026-03-23T10:15:30.000+02:00")

	tests := []struct {
		name            string
		data            map[string]any
		wantErr         string
		emptyData       bool
		wantRec         func(t *testing.T) arrow.RecordBatch
		wantLogContains []string
		wantNoErrorLogs bool
	}{
		{
			name: "happy path",
			data: map[string]any{
				"string": "a", "float32": float32(1.25), "float64": float64(1.25),
				"uint": uint(20), "uint8": uint8(20), "uint16": uint16(20),
				"uint32": uint32(20), "uint64": uint64(20), "int": int(20),
				"int8": int8(20), "int16": int16(20), "int32": int32(20),
				"int64": int64(20), "boolean": true, "time": ts,
			},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					{Name: "float32", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					{Name: "float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int16", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int32", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
					{Name: "uint", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint16", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint32", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(func() { rb.Release() })

				rb.Field(0).(*array.BooleanBuilder).Append(true)
				rb.Field(1).(*array.Float64Builder).Append(1.25)
				rb.Field(2).(*array.Float64Builder).Append(1.25)
				rb.Field(3).(*array.Int64Builder).Append(20)
				rb.Field(4).(*array.Int64Builder).Append(20)
				rb.Field(5).(*array.Int64Builder).Append(20)
				rb.Field(6).(*array.Int64Builder).Append(20)
				rb.Field(7).(*array.Int64Builder).Append(20)
				rb.Field(8).(*array.StringBuilder).Append("a")
				rb.Field(9).(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMilli()))
				rb.Field(10).(*array.Int64Builder).Append(20)
				rb.Field(11).(*array.Int64Builder).Append(20)
				rb.Field(12).(*array.Int64Builder).Append(20)
				rb.Field(13).(*array.Int64Builder).Append(20)
				rb.Field(14).(*array.Int64Builder).Append(20)

				rec := rb.NewRecordBatch()
				t.Cleanup(func() { rec.Release() })
				return rec
			},
		},
		{
			name:    "unsupported type",
			data:    map[string]any{"a": []any{1}},
			wantErr: "unsupported type",
		},
		{
			name:            "nil value",
			data:            map[string]any{"a": nil},
			emptyData:       true,
			wantNoErrorLogs: true,
		},
		{
			name: "one nil value",
			data: map[string]any{"a": 1, "b": nil},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(func() { rb.Release() })

				rb.Field(0).(*array.Int64Builder).Append(1)
				rec := rb.NewRecordBatch()
				t.Cleanup(func() { rec.Release() })
				return rec
			},
			wantNoErrorLogs: true,
		},
	}
	for _, tt := range tests {
		fl.errorf = nil
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildArrowData(ctx, tt.data, nil)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}

			if len(tt.wantLogContains) != 0 {
				require.Equal(t, tt.wantLogContains, fl.errorf)
			}

			if tt.wantNoErrorLogs {
				require.Empty(t, fl.errorf)
			}

			if tt.emptyData {
				require.NoError(t, err)
				require.Nil(t, got)
				return
			}

			require.NoError(t, err)
			defer got.Release()

			want := tt.wantRec(t)
			require.True(t, array.RecordEqual(want, got))
		})
	}
}

func TestCollect(t *testing.T) {
	ctx, _ := newMockCtxWithFakeLogger("collect", "op")

	makeSink := func() (*DuckLakeSink, *fakeConn, *fakeArrowViewManager, *int) {
		fconn := &fakeConn{}
		fdb := &fakeDB{}
		fav := &fakeArrowViewManager{}
		buildCalls := 0

		d := &DuckLakeSink{
			db:       fdb,
			conn:     fconn,
			arrowMgr: fav,
			buildArrowDataFn: func(ctx api.StreamContext, got map[string]any, fields []string) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowData(ctx, got, fields)
			},
			buildArrowDataListFn: func(ctx api.StreamContext, got []map[string]any, fields []string) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowDataList(ctx, got, fields)
			},
			conf: c{
				ducklakeName: "the_ducklake",
			},
		}
		_ = d.Provision(ctx, map[string]any{"table": "table"})
		return d, fconn, fav, &buildCalls
	}

	tests := []struct {
		name           string
		setup          func(d *DuckLakeSink, fdb *fakeConn, fav *fakeArrowViewManager, buildCalls *int)
		data           map[string]any
		wantErr        string
		wantBuildCalls int
		wantDBQueries  []string
		wantViewCalls  int
		wantReleased   bool
		fields         []string
	}{
		{
			name:           "happy path",
			data:           map[string]any{"t": int64(20), "u": float64(13.5)},
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO the_ducklake.table (t, u) SELECT t, u FROM __ekuiper_ducklake_1;"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name:           "happy path - specify fields",
			data:           map[string]any{"t": int64(20), "u": float64(13.5), "z": float64(1.5)},
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO the_ducklake.table (t, u) SELECT t, u FROM __ekuiper_ducklake_1;"},
			wantViewCalls:  1,
			wantReleased:   true,
			fields:         []string{"t", "u"},
		},
		{
			name: "error: buildArrowDataFn returns error",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, buildCalls *int) {
				s.buildArrowDataFn = func(api.StreamContext, map[string]any, []string) (arrow.RecordBatch, error) {
					(*buildCalls)++
					return nil, fmt.Errorf("error")
				}
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "arrow build failed",
			wantBuildCalls: 1,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: arrowMgr RegisterRecordBatch fails",
			setup: func(_ *DuckLakeSink, _ *fakeConn, fav *fakeArrowViewManager, _ *int) {
				fav.registerErr = fmt.Errorf("error")
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "arrow register view failed",
			wantBuildCalls: 1,
			wantDBQueries:  nil,
			wantViewCalls:  1,
			wantReleased:   false,
		},
		{
			name: "error: db exec fails",
			setup: func(_ *DuckLakeSink, fconn *fakeConn, _ *fakeArrowViewManager, _ *int) {
				fconn.errStr = "exec failed"
				fconn.numCorrectQueries = 1
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "db query execution failed",
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO the_ducklake.table (t) SELECT t FROM __ekuiper_ducklake_1;"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name: "error: conn not set",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, _ *int) {
				s.conn = nil
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "conn not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: arrowMgr not set",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, _ *int) {
				s.arrowMgr = nil
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "arrow view manager not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: buildArrowDataFn not set",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, _ *int) {
				s.buildArrowDataFn = nil
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "function build arrow data not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name:           "empty data",
			data:           map[string]any{},
			wantBuildCalls: 1,
			wantReleased:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, fconn, fav, buildCalls := makeSink()
			if tt.setup != nil {
				tt.setup(s, fconn, fav, buildCalls)
			}
			if len(tt.fields) > 0 {
				s.conf.Fields = tt.fields
			}
			err := s.Collect(ctx, testTuple{m: tt.data})

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.wantBuildCalls, *buildCalls)
			require.Equal(t, tt.wantDBQueries, fconn.queries)
			require.Equal(t, tt.wantViewCalls, fav.calls)
			require.Equal(t, tt.wantReleased, fav.released)
		})
	}
}

func TestBuildArrowDataList(t *testing.T) {
	ctx, fl := newMockCtxWithFakeLogger("buildArrowDataList", "op")
	ts1, _ := time.Parse(time.RFC3339, "2026-03-23T10:15:30.000+02:00")
	ts2 := ts1.Add(2 * time.Second)

	type tc struct {
		name            string
		rows            []map[string]any
		wantErr         string
		wantRec         func(t *testing.T) arrow.RecordBatch
		wantNil         bool
		wantLogContains []string
		wantNoErrorLogs bool
	}

	tests := []tc{
		{
			name: "happy path with type conversion",
			rows: []map[string]any{
				{
					"string": "a", "float32": float32(1.25), "float64": float64(1.25),
					"uint": uint(20), "uint8": uint8(20), "uint16": uint16(20),
					"uint32": uint32(20), "uint64": uint64(20), "int": int(20),
					"int8": int8(20), "int16": int16(20), "int32": int32(20),
					"int64": int64(20), "boolean": true, "time": ts1,
				},
				{
					"string": "b", "float32": float32(2.5), "float64": float64(2.5),
					"uint": uint(40), "uint8": uint8(40), "uint16": uint16(40),
					"uint32": uint32(40), "uint64": uint64(40), "int": int(40),
					"int8": int8(40), "int16": int16(40), "int32": int32(40),
					"int64": int64(40), "boolean": false, "time": ts2,
				},
			},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					{Name: "float32", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					{Name: "float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					{Name: "int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int16", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int32", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
					{Name: "uint", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint8", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint16", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint32", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "uint64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)
				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				// row 1
				rb.Field(0).(*array.BooleanBuilder).Append(true)
				rb.Field(1).(*array.Float64Builder).Append(1.25)
				rb.Field(2).(*array.Float64Builder).Append(1.25)
				rb.Field(3).(*array.Int64Builder).Append(20)
				rb.Field(4).(*array.Int64Builder).Append(20)
				rb.Field(5).(*array.Int64Builder).Append(20)
				rb.Field(6).(*array.Int64Builder).Append(20)
				rb.Field(7).(*array.Int64Builder).Append(20)
				rb.Field(8).(*array.StringBuilder).Append("a")
				rb.Field(9).(*array.TimestampBuilder).Append(arrow.Timestamp(ts1.UnixMilli()))
				rb.Field(10).(*array.Int64Builder).Append(20)
				rb.Field(11).(*array.Int64Builder).Append(20)
				rb.Field(12).(*array.Int64Builder).Append(20)
				rb.Field(13).(*array.Int64Builder).Append(20)
				rb.Field(14).(*array.Int64Builder).Append(20)

				// row 2
				rb.Field(0).(*array.BooleanBuilder).Append(false)
				rb.Field(1).(*array.Float64Builder).Append(2.5)
				rb.Field(2).(*array.Float64Builder).Append(2.5)
				rb.Field(3).(*array.Int64Builder).Append(40)
				rb.Field(4).(*array.Int64Builder).Append(40)
				rb.Field(5).(*array.Int64Builder).Append(40)
				rb.Field(6).(*array.Int64Builder).Append(40)
				rb.Field(7).(*array.Int64Builder).Append(40)
				rb.Field(8).(*array.StringBuilder).Append("b")
				rb.Field(9).(*array.TimestampBuilder).Append(arrow.Timestamp(ts2.UnixMilli()))
				rb.Field(10).(*array.Int64Builder).Append(40)
				rb.Field(11).(*array.Int64Builder).Append(40)
				rb.Field(12).(*array.Int64Builder).Append(40)
				rb.Field(13).(*array.Int64Builder).Append(40)
				rb.Field(14).(*array.Int64Builder).Append(40)

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name: "missing field -> null",
			rows: []map[string]any{
				{"integer": int64(1), "string": "x"},
				{"integer": int64(2)},
			},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				// schema: union of the non-nil keys of the batch
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).Append(1)
				rb.Field(1).(*array.StringBuilder).Append("x")

				rb.Field(0).(*array.Int64Builder).Append(2)
				rb.Field(1).(*array.StringBuilder).AppendNull()

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name: "type mismatch vs first row: integer",
			rows: []map[string]any{
				{"integer": int64(1)},
				{"integer": "string"},
			},
			wantErr: "type mismatch",
		},
		{
			name: "type mismatch vs first row: float",
			rows: []map[string]any{
				{"float": float64(1)},
				{"float": "string"},
			},
			wantErr: "type mismatch",
		},
		{
			name: "type mismatch vs first row: bool",
			rows: []map[string]any{
				{"bool": true},
				{"bool": "string"},
			},
			wantErr: "type mismatch",
		},
		{
			name: "type mismatch vs first row: string",
			rows: []map[string]any{
				{"string": "string"},
				{"string": true},
			},
			wantErr: "type mismatch",
		},
		{
			name: "type mismatch vs first row: timestamp",
			rows: []map[string]any{
				{"time": ts1},
				{"time": true},
			},
			wantErr: "type mismatch",
		},
		{
			name:            "null arrow data: empty data",
			rows:            []map[string]any{},
			wantNil:         true,
			wantLogContains: []string{"Ducklake sink: empty data"},
		},
		{
			name:            "null arrow data: empty first row",
			rows:            []map[string]any{{"a": nil}},
			wantNil:         true,
			wantNoErrorLogs: true,
		},
		{
			name:            "null arrow data: empty value first row",
			rows:            []map[string]any{{"t": nil}, {"t": int64(40)}},
			wantNoErrorLogs: true,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				// schema from the non-nil values of the whole batch
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "t", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).AppendNull()
				rb.Field(0).(*array.Int64Builder).Append(40)

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name:    "null arrow data: empty value second row",
			rows:    []map[string]any{{"t": int64(20)}, {"t": nil}},
			wantNil: false,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				// schema deduced from the batch, not from the first row
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "t", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).Append(20)
				rb.Field(0).(*array.Int64Builder).AppendNull()

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
			wantNoErrorLogs: true,
		},
		{
			name:            "null arrow data: empty value first row second field",
			rows:            []map[string]any{{"a": int64(20), "b": nil}, {"a": int64(20), "b": int64(40)}},
			wantNoErrorLogs: true,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				// schema: union of the non-nil keys of the batch, sorted
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "b", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).Append(20)
				rb.Field(1).(*array.Int64Builder).AppendNull()

				rb.Field(0).(*array.Int64Builder).Append(20)
				rb.Field(1).(*array.Int64Builder).Append(40)

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name: "sparse batch no fields",
			rows: []map[string]any{
				{"device_instance_id": "powerhub", "battery_level": "93", "ingested_at": "2026-03-23T10:15:30Z"},
				{"device_instance_id": "powerhub", "battery_voltage": "8156", "ingested_at": "2026-03-23T10:15:32Z"},
				{"device_instance_id": "powerhub", "battery_current": "67", "ingested_at": "2026-03-23T10:15:34Z"},
			},
			wantNoErrorLogs: true,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "battery_current", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "battery_level", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "battery_voltage", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "device_instance_id", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "ingested_at", Type: arrow.BinaryTypes.String, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				// row 0
				rb.Field(0).(*array.StringBuilder).AppendNull()
				rb.Field(1).(*array.StringBuilder).Append("93")
				rb.Field(2).(*array.StringBuilder).AppendNull()
				rb.Field(3).(*array.StringBuilder).Append("powerhub")
				rb.Field(4).(*array.StringBuilder).Append("2026-03-23T10:15:30Z")

				// row 1
				rb.Field(0).(*array.StringBuilder).AppendNull()
				rb.Field(1).(*array.StringBuilder).AppendNull()
				rb.Field(2).(*array.StringBuilder).Append("8156")
				rb.Field(3).(*array.StringBuilder).Append("powerhub")
				rb.Field(4).(*array.StringBuilder).Append("2026-03-23T10:15:32Z")

				// row 2
				rb.Field(0).(*array.StringBuilder).Append("67")
				rb.Field(1).(*array.StringBuilder).AppendNull()
				rb.Field(2).(*array.StringBuilder).AppendNull()
				rb.Field(3).(*array.StringBuilder).Append("powerhub")
				rb.Field(4).(*array.StringBuilder).Append("2026-03-23T10:15:34Z")

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name:            "no error logs on missing value",
			rows:            []map[string]any{{"t": int64(20)}, {"t": nil}},
			wantNoErrorLogs: true,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "t", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).Append(20)
				rb.Field(0).(*array.Int64Builder).AppendNull()

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name:            "empty map row skipped",
			rows:            []map[string]any{{}, {"a": int64(20)}},
			wantNoErrorLogs: true,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).Append(20)

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fl.errorf = nil
			fl.warnf = nil
			got, err := buildArrowDataList(ctx, tt.rows, nil)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}

			require.NoError(t, err)

			if tt.wantNil {
				require.Nil(t, got)
			}

			if len(tt.wantLogContains) != 0 {
				require.Equal(t, tt.wantLogContains, fl.errorf)
			}

			if len(tt.wantLogContains) == 0 && !tt.wantNil {
				require.NotNil(t, got)
				defer got.Release()

				want := tt.wantRec(t)
				require.True(t, array.RecordEqual(want, got))
			}

			if tt.wantNoErrorLogs {
				require.Empty(t, fl.errorf)
			}

			require.Empty(t, fl.warnf)
		})
	}
}

func TestCollectList(t *testing.T) {
	ctx, _ := newMockCtxWithFakeLogger("collectList", "op")

	makeSink := func() (*DuckLakeSink, *fakeConn, *fakeArrowViewManager, *int, *[]string) {
		fconn := &fakeConn{}
		fav := &fakeArrowViewManager{}
		buildCalls := 0
		var lastFields []string

		d := &DuckLakeSink{
			conn:     fconn,
			arrowMgr: fav,
			buildArrowDataListFn: func(ctx api.StreamContext, got []map[string]any, fields []string) (arrow.RecordBatch, error) {
				buildCalls++
				lastFields = fields
				return buildArrowDataList(ctx, got, fields)
			},
			buildArrowDataFn: func(ctx api.StreamContext, got map[string]any, fields []string) (arrow.RecordBatch, error) {
				buildCalls++
				lastFields = fields
				return buildArrowData(ctx, got, fields)
			},
			conf: c{
				ducklakeName: "the_ducklake",
			},
		}
		_ = d.Provision(ctx, map[string]any{"table": "table"})
		return d, fconn, fav, &buildCalls, &lastFields
	}

	tests := []struct {
		name            string
		setup           func(d *DuckLakeSink, fconn *fakeConn, fav *fakeArrowViewManager, buildCalls *int)
		data            []map[string]any
		wantErr         string
		wantBuildCalls  int
		wantDBQueries   []string
		wantViewCalls   int
		wantReleased    bool
		fields          []string
		wantBuildFields []string
	}{
		{
			name:           "happy path",
			data:           []map[string]any{{"t": int64(20), "u": float64(13.5)}, {"t": int64(40), "u": float64(26.5)}},
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO the_ducklake.table (t, u) SELECT t, u FROM __ekuiper_ducklake_1;"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name:            "happy path - specify fields",
			data:            []map[string]any{{"t": int64(20), "u": float64(13.5), "z": float64(1.5)}, {"t": int64(40), "u": float64(26.5), "z": float64(2.5)}},
			wantBuildCalls:  1,
			wantDBQueries:   []string{"INSERT INTO the_ducklake.table (t, u) SELECT t, u FROM __ekuiper_ducklake_1;"},
			wantViewCalls:   1,
			wantReleased:    true,
			fields:          []string{"t", "u"},
			wantBuildFields: []string{"t", "u"},
		},
		{
			name: "error: buildArrowDataListFn returns error",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, buildCalls *int) {
				s.buildArrowDataListFn = func(api.StreamContext, []map[string]any, []string) (arrow.RecordBatch, error) {
					(*buildCalls)++
					return nil, fmt.Errorf("error")
				}
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "arrow build failed",
			wantBuildCalls: 1,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: arrowMgr RegisterRecordBatch fails",
			setup: func(_ *DuckLakeSink, _ *fakeConn, fav *fakeArrowViewManager, _ *int) {
				fav.registerErr = fmt.Errorf("error")
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "arrow register view failed",
			wantBuildCalls: 1,
			wantDBQueries:  nil,
			wantViewCalls:  1,
			wantReleased:   false,
		},
		{
			name: "error: db exec fails",
			setup: func(_ *DuckLakeSink, fconn *fakeConn, _ *fakeArrowViewManager, _ *int) {
				fconn.errStr = "exec failed"
				fconn.numCorrectQueries = 1
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "db query execution failed",
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO the_ducklake.table (t) SELECT t FROM __ekuiper_ducklake_1;"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name: "error: conn not set",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, _ *int) {
				s.conn = nil
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "conn not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: arrowMgr not set",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, _ *int) {
				s.arrowMgr = nil
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "arrow view manager not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: buildArrowDataListFn not set",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, _ *int) {
				s.buildArrowDataListFn = nil
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "function build arrow data list not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name:           "empty data",
			data:           []map[string]any{},
			wantBuildCalls: 1,
			wantReleased:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, fconn, fav, buildCalls, lastFields := makeSink()
			if tt.setup != nil {
				tt.setup(s, fconn, fav, buildCalls)
			}
			if len(tt.fields) > 0 {
				s.conf.Fields = tt.fields
			}

			err := s.CollectList(ctx, testTupleList{ms: tt.data})

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.wantBuildCalls, *buildCalls)
			require.Equal(t, tt.wantDBQueries, fconn.queries)
			require.Equal(t, tt.wantViewCalls, fav.calls)
			require.Equal(t, tt.wantReleased, fav.released)
			if tt.wantBuildFields != nil {
				require.Equal(t, tt.wantBuildFields, *lastFields)
			}
		})
	}
}

// TestCollectList_InsertUsesArrowSchema is a regression guard: the INSERT column
// list must follow the arrow batch schema, never conf.Fields.
func TestCollectList_InsertUsesArrowSchema(t *testing.T) {
	ctx := mockContext.NewMockContext("collectList arrow schema", "op")

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	t.Cleanup(func() { mem.AssertSize(t, 0) })

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	t.Cleanup(rb.Release)
	rb.Field(0).(*array.Int64Builder).Append(7)

	fconn := &fakeConn{}
	s := &DuckLakeSink{
		conn:             fconn,
		arrowMgr:         &fakeArrowViewManager{},
		buildArrowDataFn: buildArrowData,
		buildArrowDataListFn: func(api.StreamContext, []map[string]any, []string) (arrow.RecordBatch, error) {
			return rb.NewRecordBatch(), nil
		},
		conf: c{
			ducklakeName:   "the_ducklake",
			sanitizedTable: "table",
			Fields:         []string{"x"},
		},
	}

	err := s.CollectList(ctx, testTupleList{ms: []map[string]any{{"a": int64(7)}}})
	require.NoError(t, err)
	require.Equal(t, []string{"INSERT INTO the_ducklake.table (a) SELECT a FROM __ekuiper_ducklake_1;"}, fconn.queries)
}

func TestCollect_InMemoryDB(t *testing.T) {
	ctx := mockContext.NewMockContext("collect in memory", "op")

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	db.ExecContext(ctx, "CREATE TABLE prova (a BIGINT, b FLOAT);")

	arrowMgr, err := newArrowManager(conn)
	require.NoError(t, err)

	s := &DuckLakeSink{
		conn:                 conn,
		arrowMgr:             arrowMgr,
		buildArrowDataFn:     buildArrowData,
		buildArrowDataListFn: buildArrowDataList,
		conf: c{
			ducklakeName:   "memory",
			sanitizedTable: "prova",
		},
	}

	data := testTuple{map[string]any{"a": int64(20), "b": float64(12.5)}}
	err = s.Collect(ctx, data)
	require.NoError(t, err)

	var gota int64
	var gotb float64

	require.NoError(
		t,
		conn.QueryRowContext(ctx, "SELECT a, b FROM prova").Scan(&gota, &gotb),
	)
	require.Equal(t, int64(20), gota)
	require.Equal(t, float64(12.5), gotb)
}

func TestCollectList_InMemoryDB(t *testing.T) {
	ctx := mockContext.NewMockContext("collect in memory", "op")

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	db.ExecContext(ctx, "CREATE TABLE prova (a BIGINT, b FLOAT);")

	arrowMgr, err := newArrowManager(conn)
	require.NoError(t, err)

	s := &DuckLakeSink{
		conn:                 conn,
		arrowMgr:             arrowMgr,
		buildArrowDataFn:     buildArrowData,
		buildArrowDataListFn: buildArrowDataList,
		conf: c{
			ducklakeName:   "memory",
			sanitizedTable: "prova",
		},
	}

	data := testTupleList{
		[]map[string]any{
			{"a": int64(20), "b": float64(12.5)},
			{"a": int64(40), "b": float64(24.5)},
		},
	}
	err = s.CollectList(ctx, data)
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "SELECT a, b FROM prova ORDER BY a")
	var gota []int64
	var gotb []float64
	for rows.Next() {
		var vint int64
		var vfloat float64
		require.NoError(t, rows.Scan(&vint, &vfloat))
		gota = append(gota, vint)
		gotb = append(gotb, vfloat)
	}
	require.Equal(t, []int64{20, 40}, gota)
	require.Equal(t, []float64{12.5, 24.5}, gotb)
}

func TestCollectList_InMemoryDB_SparsePivot(t *testing.T) {
	ctx := mockContext.NewMockContext("collect sparse pivot in memory", "op")

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.ExecContext(ctx, "CREATE TABLE sparse_pivot (device_instance_id VARCHAR, battery_level VARCHAR, battery_voltage VARCHAR, battery_current VARCHAR, ingested_at VARCHAR);")
	require.NoError(t, err)

	arrowMgr, err := newArrowManager(conn)
	require.NoError(t, err)

	s := &DuckLakeSink{
		conn:                 conn,
		arrowMgr:             arrowMgr,
		buildArrowDataFn:     buildArrowData,
		buildArrowDataListFn: buildArrowDataList,
		conf: c{
			ducklakeName:   "memory",
			sanitizedTable: "sparse_pivot",
			Fields:         []string{"device_instance_id", "battery_level", "battery_voltage", "battery_current", "ingested_at"},
		},
	}

	data := testTupleList{
		[]map[string]any{
			{"device_instance_id": "powerhub", "battery_level": "93", "ingested_at": "2026-03-23T10:15:30Z"},
			{"device_instance_id": "powerhub", "battery_voltage": "8156", "ingested_at": "2026-03-23T10:15:32Z"},
			{"device_instance_id": "powerhub", "battery_current": "67", "ingested_at": "2026-03-23T10:15:34Z"},
		},
	}
	err = s.CollectList(ctx, data)
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "SELECT device_instance_id, battery_level, battery_voltage, battery_current FROM sparse_pivot ORDER BY ingested_at")
	require.NoError(t, err)
	defer rows.Close()

	// expected value per column, in ingested_at order: each row carries exactly one reading
	wantLevels := []sql.NullString{{String: "93", Valid: true}, {}, {}}
	wantVoltages := []sql.NullString{{}, {String: "8156", Valid: true}, {}}
	wantCurrents := []sql.NullString{{}, {}, {String: "67", Valid: true}}

	rowNum := 0
	for rows.Next() {
		require.Less(t, rowNum, len(wantLevels))
		var id, level, voltage, current sql.NullString
		require.NoError(t, rows.Scan(&id, &level, &voltage, &current))
		require.True(t, id.Valid)
		require.Equal(t, "powerhub", id.String)
		require.Equal(t, wantLevels[rowNum], level)
		require.Equal(t, wantVoltages[rowNum], voltage)
		require.Equal(t, wantCurrents[rowNum], current)
		rowNum++
	}
	require.NoError(t, rows.Err())
	require.Equal(t, 3, rowNum)
}

func TestCollectList_DeclaredFields(t *testing.T) {
	ctx, fl := newMockCtxWithFakeLogger("collectDeclaredFields", "op")

	tests := []struct {
		name             string
		fields           []string
		data             []map[string]any
		wantQuery        string
		wantWarnContains []string
		wantWarnOrdered  []string
	}{
		{
			name:             "extra key dropped with single warning",
			fields:           []string{"a"},
			data:             []map[string]any{{"a": int64(1), "z": int64(9)}, {"z": int64(9)}},
			wantQuery:        "INSERT INTO the_ducklake.table (a) SELECT a FROM __ekuiper_ducklake_1;",
			wantWarnContains: []string{"undeclared", "z"},
		},
		{
			name:             "insert lists all declared columns",
			fields:           []string{"a", "b"},
			data:             []map[string]any{{"a": int64(1)}, {"a": int64(2)}},
			wantQuery:        "INSERT INTO the_ducklake.table (a, b) SELECT a, b FROM __ekuiper_ducklake_1;",
			wantWarnContains: nil,
		},
		{
			name:             "declared order preserved in insert",
			fields:           []string{"u", "t"},
			data:             []map[string]any{{"u": int64(1), "t": int64(2)}},
			wantQuery:        "INSERT INTO the_ducklake.table (u, t) SELECT u, t FROM __ekuiper_ducklake_1;",
			wantWarnContains: nil,
		},
		{
			name:            "two undeclared keys warn once each in order",
			fields:          []string{"a"},
			data:            []map[string]any{{"a": int64(1), "z": int64(9), "y": int64(8)}},
			wantQuery:       "INSERT INTO the_ducklake.table (a) SELECT a FROM __ekuiper_ducklake_1;",
			wantWarnOrdered: []string{"<y>", "<z>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fl.errorf = nil
			fl.warnf = nil

			fconn := &fakeConn{}
			s := &DuckLakeSink{
				conn:                 fconn,
				arrowMgr:             &fakeArrowViewManager{},
				buildArrowDataFn:     buildArrowData,
				buildArrowDataListFn: buildArrowDataList,
				conf: c{
					ducklakeName:   "the_ducklake",
					sanitizedTable: "table",
					Fields:         tt.fields,
				},
			}

			err := s.CollectList(ctx, testTupleList{ms: tt.data})
			require.NoError(t, err)
			require.Empty(t, fl.errorf)
			require.Equal(t, []string{tt.wantQuery}, fconn.queries)

			if len(tt.wantWarnOrdered) > 0 {
				require.Len(t, fl.warnf, len(tt.wantWarnOrdered))
				for i, substr := range tt.wantWarnOrdered {
					require.Contains(t, fl.warnf[i], substr)
				}
			} else if len(tt.wantWarnContains) > 0 {
				require.Len(t, fl.warnf, 1)
				for _, substr := range tt.wantWarnContains {
					require.Contains(t, fl.warnf[0], substr)
				}
			} else {
				require.Empty(t, fl.warnf)
			}
		})
	}
}

func TestCollectList_InMemoryDB_DeclaredFieldWithoutValues(t *testing.T) {
	ctx := mockContext.NewMockContext("collect declared field without values in memory", "op")

	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.ExecContext(ctx, "CREATE TABLE declared_null (a BIGINT, b VARCHAR);")
	require.NoError(t, err)

	arrowMgr, err := newArrowManager(conn)
	require.NoError(t, err)

	s := &DuckLakeSink{
		conn:                 conn,
		arrowMgr:             arrowMgr,
		buildArrowDataFn:     buildArrowData,
		buildArrowDataListFn: buildArrowDataList,
		conf: c{
			ducklakeName:   "memory",
			sanitizedTable: "declared_null",
			Fields:         []string{"a", "b"},
		},
	}

	data := testTupleList{ms: []map[string]any{{"a": int64(1)}, {"a": int64(2)}}}
	err = s.CollectList(ctx, data)
	require.NoError(t, err)

	rows, err := conn.QueryContext(ctx, "SELECT a, b FROM declared_null ORDER BY a")
	require.NoError(t, err)
	defer rows.Close()

	var gots []int64
	for rows.Next() {
		var a int64
		var b sql.NullString
		require.NoError(t, rows.Scan(&a, &b))
		require.False(t, b.Valid)
		gots = append(gots, a)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []int64{1, 2}, gots)
}

func TestDuckLakeSink_Info(t *testing.T) {
	t.Run("fields declared", func(t *testing.T) {
		s := &DuckLakeSink{conf: c{Fields: []string{"a", "b"}}}
		info := s.Info()
		require.True(t, info.HasFields)
		require.False(t, info.HasBatch)
	})

	t.Run("no fields", func(t *testing.T) {
		s := &DuckLakeSink{}
		info := s.Info()
		require.False(t, info.HasFields)
		require.False(t, info.HasBatch)
	})
}

func TestArrowSchemaFromFields(t *testing.T) {
	tests := []struct {
		name    string
		fields  []string
		rows    []map[string]any
		want    *inferredSchema
		wantErr string
	}{
		{
			name:   "keeps declared order and derives types",
			fields: []string{"b", "a", "c"},
			rows: []map[string]any{
				{"a": int64(1), "c": nil},
				{"b": "x", "a": int64(2)},
			},
			want: &inferredSchema{
				keys: []string{"b", "a", "c"},
				dts:  []arrow.DataType{arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String},
			},
		},
		{
			name:   "declared column never valued defaults to string",
			fields: []string{"a", "b"},
			rows: []map[string]any{
				{"a": int64(1)},
				{"a": int64(2)},
			},
			want: &inferredSchema{
				keys: []string{"a", "b"},
				dts:  []arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String},
			},
		},
		{
			name:   "undeclared key produces no column",
			fields: []string{"a"},
			rows:   []map[string]any{{"a": int64(1), "z": int64(9)}},
			want: &inferredSchema{
				keys: []string{"a"},
				dts:  []arrow.DataType{arrow.PrimitiveTypes.Int64},
			},
		},
		{
			name:    "unsupported value type",
			fields:  []string{"a", "b"},
			rows:    []map[string]any{{"a": int64(1), "b": []any{1}}},
			wantErr: "field <b> has unsupported type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := arrowSchemaFromFields(tt.fields, tt.rows)

			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, tt.want.keys, got.keys)
			require.Equal(t, tt.want.dts, got.dts)
			require.NotNil(t, got.schema)
			require.Equal(t, len(tt.want.keys), got.schema.NumFields())

			fieldNames := make([]string, 0, got.schema.NumFields())
			for _, f := range got.schema.Fields() {
				fieldNames = append(fieldNames, f.Name)
				require.True(t, f.Nullable)
			}
			require.Equal(t, tt.want.keys, fieldNames)
		})
	}
}

func TestInferArrowSchemaFromRows(t *testing.T) {
	tests := []struct {
		name string
		rows []map[string]any
		want *inferredSchema
	}{
		{
			name: "union sorted alphabetically with first seen type",
			rows: []map[string]any{
				{"b": int64(1)},
				{"a": "x"},
			},
			want: &inferredSchema{
				keys: []string{"a", "b"},
				dts:  []arrow.DataType{arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64},
			},
		},
		{
			name: "no usable value returns nil for empty rows",
			rows: []map[string]any{},
		},
		{
			name: "no usable value returns nil for only nil values",
			rows: []map[string]any{{"a": nil}, {}},
		},
		{
			name: "single row regression",
			rows: []map[string]any{{"t": int64(20), "u": float64(13.5)}},
			want: &inferredSchema{
				keys: []string{"t", "u"},
				dts:  []arrow.DataType{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Float64},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := inferArrowSchemaFromRows(tt.rows)
			require.NoError(t, err)

			if tt.want == nil {
				require.Nil(t, got)
				return
			}

			require.NotNil(t, got)
			require.Equal(t, tt.want.keys, got.keys)
			require.Equal(t, tt.want.dts, got.dts)
			require.NotNil(t, got.schema)
			require.Equal(t, len(tt.want.keys), got.schema.NumFields())

			fieldNames := make([]string, 0, got.schema.NumFields())
			for _, f := range got.schema.Fields() {
				fieldNames = append(fieldNames, f.Name)
				require.True(t, f.Nullable)
			}
			require.Equal(t, tt.want.keys, fieldNames)
		})
	}
}
