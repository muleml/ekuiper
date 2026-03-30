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
}

func (l *fakeLogger) Debug(args ...any)     {}
func (l *fakeLogger) Debugf(string, ...any) {}
func (l *fakeLogger) Info(args ...any)      {}
func (l *fakeLogger) Infof(string, ...any)  {}
func (l *fakeLogger) Warn(args ...any)      {}
func (l *fakeLogger) Warnf(string, ...any)  {}
func (l *fakeLogger) Error(args ...any)     {}
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
				"table": "table",
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
				"table": "table",
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
				"table": "table",
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
				"table": `my table "v1"`,
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE the_ducklake;",
			},
			useFakeConn: true,
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
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH 'metadata.duckdb', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE the_ducklake;",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE the_ducklake;",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 6,
			useFakeConn:       true,
		},
		{
			name: "error use ducklake",
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
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
				"CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password');",
				"CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '', DATA_PATH 's3://ducklake', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'});",
				"ATTACH 'ducklake:ducklake_secret' AS the_ducklake;",
				"USE the_ducklake;",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 7,
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"catalog": map[string]any{
					"catalog_type": "duckdb",
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
				},
				"table": "table",
			},
			expected: []string{
				"INSTALL ducklake;",
				"INSTALL postgres;",
				"CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000');",
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
			err := s.Provision(ctx, tt.conf)
			require.NoError(t, err)
			err = s.Ping(ctx, tt.conf)
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
			name:      "nil value",
			data:      map[string]any{"a": nil},
			emptyData: true,
			wantLogContains: []string{
				"Ducklake sink: empty value inferring schema, field <a>",
				"Ducklake sink: empty inferred schema",
			},
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
			wantLogContains: []string{
				"Ducklake sink: empty value inferring schema, field <b>",
			},
		},
	}
	for _, tt := range tests {
		fl.errorf = nil
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildArrowData(ctx, tt.data)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}

			if len(tt.wantLogContains) != 0 {
				require.Equal(t, tt.wantLogContains, fl.errorf)
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
			buildArrowDataFn: func(ctx api.StreamContext, got map[string]any) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowData(ctx, got)
			},
			buildArrowDataListFn: func(ctx api.StreamContext, got []map[string]any) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowDataList(ctx, got)
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
	}{
		{
			name:           "happy path",
			data:           map[string]any{"t": int64(20)},
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO table SELECT * FROM __ekuiper_ducklake_1"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name: "error: buildArrowDataFn returns error",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, buildCalls *int) {
				s.buildArrowDataFn = func(api.StreamContext, map[string]any) (arrow.RecordBatch, error) {
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
			wantDBQueries:  []string{"INSERT INTO table SELECT * FROM __ekuiper_ducklake_1"},
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

				// schema from first row
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
			name:    "null arrow data: empty first row",
			rows:    []map[string]any{{"a": nil}},
			wantNil: true,
			wantLogContains: []string{
				"Ducklake sink: empty value inferring schema, field <a>",
				"Ducklake sink: empty inferred schema",
			},
		},
		{
			name:    "null arrow data: empty value first row",
			rows:    []map[string]any{{"t": nil}, {"t": int64(40)}},
			wantNil: true,
			wantLogContains: []string{
				"Ducklake sink: empty value inferring schema, field <t>",
				"Ducklake sink: empty inferred schema",
			},
		},
		{
			name:    "null arrow data: empty value second row",
			rows:    []map[string]any{{"t": int64(20)}, {"t": nil}},
			wantNil: false,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				// schema from first row
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
			wantLogContains: []string{
				"Ducklake sink: empty value Row <1> Field <t>",
			},
		},
		{
			name:    "null arrow data: empty value first row second field",
			rows:    []map[string]any{{"a": int64(20), "b": nil}, {"a": int64(20), "b": int64(40)}},
			wantNil: false,
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				// schema from first row
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				rb.Field(0).(*array.Int64Builder).Append(20)
				rb.Field(0).(*array.Int64Builder).Append(20)

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
			wantLogContains: []string{
				"Ducklake sink: empty value inferring schema, field <b>",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fl.errorf = nil
			got, err := buildArrowDataList(ctx, tt.rows)

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
		})
	}
}

func TestCollectList(t *testing.T) {
	ctx, _ := newMockCtxWithFakeLogger("collectList", "op")

	makeSink := func() (*DuckLakeSink, *fakeConn, *fakeArrowViewManager, *int) {
		fconn := &fakeConn{}
		fav := &fakeArrowViewManager{}
		buildCalls := 0

		d := &DuckLakeSink{
			conn:     fconn,
			arrowMgr: fav,
			buildArrowDataListFn: func(ctx api.StreamContext, got []map[string]any) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowDataList(ctx, got)
			},
			buildArrowDataFn: func(ctx api.StreamContext, got map[string]any) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowData(ctx, got)
			},
		}
		_ = d.Provision(ctx, map[string]any{"table": "table"})
		return d, fconn, fav, &buildCalls
	}

	tests := []struct {
		name           string
		setup          func(d *DuckLakeSink, fconn *fakeConn, fav *fakeArrowViewManager, buildCalls *int)
		data           []map[string]any
		wantErr        string
		wantBuildCalls int
		wantDBQueries  []string
		wantViewCalls  int
		wantReleased   bool
	}{
		{
			name:           "happy path",
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO table SELECT * FROM __ekuiper_ducklake_1"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name: "error: buildArrowDataListFn returns error",
			setup: func(s *DuckLakeSink, _ *fakeConn, _ *fakeArrowViewManager, buildCalls *int) {
				s.buildArrowDataListFn = func(api.StreamContext, []map[string]any) (arrow.RecordBatch, error) {
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
			wantDBQueries:  []string{"INSERT INTO table SELECT * FROM __ekuiper_ducklake_1"},
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
			name: "error: buildArrowDataFn not set",
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
			s, fconn, fav, buildCalls := makeSink()
			if tt.setup != nil {
				tt.setup(s, fconn, fav, buildCalls)
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
		})
	}
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
	}
	s.conf.sanitizedTable = "prova"

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
	}
	s.conf.sanitizedTable = "prova"

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
