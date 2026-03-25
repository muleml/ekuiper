package ducklake

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

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

type fakeDB struct {
	queries           []string
	errStr            string
	numCorrectQueries int
	closeCalls        int
	closeErr          string
}

func (f *fakeDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	f.queries = append(f.queries, query)
	if f.errStr != "" && len(f.queries) >= f.numCorrectQueries {
		return nil, fmt.Errorf("%s", f.errStr)
	}
	return fakeResult{}, nil
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

func (f *fakeArrowViewManager) RegisterRecordBatch(_ context.Context, name string, batch arrow.RecordBatch) (func(), error) {
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
				Table:       "table",
				quotedTable: "table",
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
				Table:       "table",
				quotedTable: "table",
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
				Table:       "table",
				quotedTable: "table",
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
				Table:       `my table "v1"`,
				quotedTable: `mytablev1`,
			},
		},
		{
			name: "invalid table name",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"ATTACH 'ducklake:' AS the_ducklake (DATA_PATH 's3://ducklake');",
				"USE the_ducklake;",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"ATTACH 'ducklake:' AS the_ducklake (DATA_PATH 's3://ducklake');",
				"USE the_ducklake;",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
				"ATTACH 'ducklake:postgres_secret' AS the_ducklake (DATA_PATH 's3://ducklake');",
				"USE the_ducklake;",
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
			errStr:            "Ducklake sink connection error",
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
			errStr:            "Ducklake sink connection error",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
			},
			errStr:            "Ducklake sink connection error",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 4,
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
				"ATTACH 'ducklake:postgres_secret' AS the_ducklake (DATA_PATH 's3://ducklake');",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 5,
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
				"ATTACH 'ducklake:postgres_secret' AS the_ducklake (DATA_PATH 's3://ducklake');",
				"USE the_ducklake;",
			},
			errStr:            "Ducklake sink connection error",
			numCorrectQueries: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &statusRecorder{}
			db := &fakeDB{errStr: tt.errStr, numCorrectQueries: tt.numCorrectQueries}
			s := &DuckLakeSink{db: db}
			err := s.Provision(ctx, tt.conf)
			require.NoError(t, err)
			err = s.Connect(ctx, rec.handler)
			if tt.errStr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errStr)
				require.Len(t, rec.calls, 2)
				require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
				require.Equal(t, api.ConnectionDisconnected, rec.calls[1].status)
				require.Equal(t, tt.expected, db.queries)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, db.queries)
			require.Len(t, rec.calls, 2)
			require.Equal(t, api.ConnectionConnecting, rec.calls[0].status)
			require.Equal(t, api.ConnectionConnected, rec.calls[1].status)
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

	t.Run("db is not closable", func(t *testing.T) {
		db := &fakeDB{closeErr: "db is not closable"}
		s := &DuckLakeSink{db: db}
		err := s.Close(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "error closing ducklake sink")
		require.ErrorContains(t, err, "db is not closable")
		require.Equal(t, 1, db.closeCalls)
	})

	t.Run("close ok", func(t *testing.T) {
		db := &fakeDB{}
		s := &DuckLakeSink{db: db}
		err := s.Close(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, db.closeCalls)
	})
}

// func TestPing(t *testing.T) {
// 	ctx := mockContext.NewMockContext("ping", "op")
//
// 	t.Run("happy path", func(t *testing.T) {
// 		fsv := &fakeStorageVerifier{}
// 		fcv := &fakeCatalogVerifier{}
// 		s := &DuckLakeSink{
// 			storageVerifier: fsv,
// 			catalogVerifier: fcv,
// 		}
// 		conf := map[string]any{
// 			"catalog": map[string]any{
// 				"catalog_type":     "postgres",
// 				"catalog_host":     "postgres",
// 				"catalog_port":     5432,
// 				"catalog_database": "ducklake_catalog",
// 				"catalog_user":     "user",
// 				"catalog_password": "password",
// 			},
// 			"storage": map[string]any{
// 				"storage_type":     "s3",
// 				"storage_endpoint": "test-endpoint:9000",
// 				"storage_bucket":   "ducklake",
// 				"storage_key_id":   "test_id",
// 				"storage_secret":   "test_secret",
// 			},
// 			"table": "table",
// 		}
// 		err := s.Ping(ctx, conf)
// 		require.NoError(t, err)
// 		require.Equal(t, 1, fsv.calls)
// 		require.Equal(t, 1, fcv.calls)
// 	})
//
// 	t.Run("error when provision fails", func(t *testing.T) {
// 		s := &DuckLakeSink{}
// 		props := map[string]any{}
// 		err := s.Ping(ctx, props)
// 		require.Error(t, err)
// 		require.ErrorContains(t, err, "Ducklake sink ping provision error")
// 	})
//
// 	t.Run("storage connection error", func(t *testing.T) {
// 		fsv := &fakeStorageVerifier{strErr: "storage connection error"}
// 		fcv := &fakeCatalogVerifier{}
// 		s := &DuckLakeSink{
// 			storageVerifier: fsv,
// 			catalogVerifier: fcv,
// 		}
// 		conf := map[string]any{
// 			"catalog": map[string]any{
// 				"catalog_type":     "postgres",
// 				"catalog_host":     "postgres",
// 				"catalog_port":     5432,
// 				"catalog_database": "ducklake_catalog",
// 				"catalog_user":     "user",
// 				"catalog_password": "password",
// 			},
// 			"storage": map[string]any{
// 				"storage_type":     "s3",
// 				"storage_endpoint": "test-endpoint:9000",
// 				"storage_bucket":   "ducklake",
// 				"storage_key_id":   "test_id",
// 				"storage_secret":   "test_secret",
// 			},
// 			"table": "table",
// 		}
// 		err := s.Ping(ctx, conf)
// 		require.Error(t, err)
// 		require.ErrorContains(t, err, "Ducklake sink ping connection error")
// 		require.ErrorContains(t, err, "storage connection error")
// 		require.Equal(t, 1, fsv.calls)
// 	})
// 	t.Run("catalog connection error", func(t *testing.T) {
// 		fsv := &fakeStorageVerifier{}
// 		fcv := &fakeCatalogVerifier{strErr: "catalog connection error"}
// 		s := &DuckLakeSink{
// 			storageVerifier: fsv,
// 			catalogVerifier: fcv,
// 		}
// 		conf := map[string]any{
// 			"catalog": map[string]any{
// 				"catalog_type":     "postgres",
// 				"catalog_host":     "postgres",
// 				"catalog_port":     5432,
// 				"catalog_database": "ducklake_catalog",
// 				"catalog_user":     "user",
// 				"catalog_password": "password",
// 			},
// 			"storage": map[string]any{
// 				"storage_type":     "s3",
// 				"storage_endpoint": "test-endpoint:9000",
// 				"storage_bucket":   "ducklake",
// 				"storage_key_id":   "test_id",
// 				"storage_secret":   "test_secret",
// 			},
// 			"table": "table",
// 		}
// 		err := s.Ping(ctx, conf)
// 		require.Error(t, err)
// 		require.ErrorContains(t, err, "Ducklake sink ping connection error")
// 		require.ErrorContains(t, err, "catalog connection error")
// 		require.Equal(t, 1, fcv.calls)
// 	})
// }

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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"ATTACH 'ducklake:' AS the_ducklake (DATA_PATH 's3://ducklake');",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"ATTACH 'ducklake:' AS the_ducklake (DATA_PATH 's3://ducklake');",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
				"ATTACH 'ducklake:postgres_secret' AS the_ducklake (DATA_PATH 's3://ducklake');",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 4,
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
				"CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID 'test_id', SECRET 'test_secret', ENDPOINT 'test-endpoint:9000')",
				"CREATE OR REPLACE postgres_secret (TYPE postgres, HOST 'postgres', PORT 5432, DATABASE ducklake_catalog, USER 'user', PASSWORD 'password')",
				"ATTACH 'ducklake:postgres_secret' AS the_ducklake (DATA_PATH 's3://ducklake');",
			},
			errStr:            "Ducklake sink ping connection error",
			numCorrectQueries: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := &fakeDB{errStr: tt.errStr, numCorrectQueries: tt.numCorrectQueries}
			s := &DuckLakeSink{db: db}
			err := s.Provision(ctx, tt.conf)
			require.NoError(t, err)
			err = s.Ping(ctx, tt.conf)
			if tt.errStr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.errStr)
				require.Equal(t, tt.expected, db.queries)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, db.queries)
		})
	}
}

func TestValidateIdentLoose(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantErr string
	}{
		{name: "empty", in: "", wantErr: "empty"},
		{name: "semicolon", in: "t; drop table x", wantErr: "contains ';'"},
		{name: "newline", in: "t\nx", wantErr: "control"},
		{name: "tab", in: "t\tx", wantErr: "control"},
		{name: "ok simple", in: "table_1"},
		{name: "ok with space (will require quoting)", in: "my table"},
		{name: "ok with quote (will be escaped)", in: `a"b`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIdentLoose(tt.in)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBuildArrowData(t *testing.T) {
	ctx, _ := newMockCtxWithFakeLogger("collect", "op")

	ts, _ := time.Parse(time.RFC3339, "2026-03-23T10:15:30.000+02:00")

	tests := []struct {
		name      string
		data      map[string]any
		wantErr   string
		emptyData bool
		wantRec   func(t *testing.T) arrow.RecordBatch
	}{
		{
			name: "happy path",
			data: map[string]any{
				"string":  "string",
				"float":   1.25,
				"integer": int64(20),
				"boolean": true,
				"time":    ts,
			},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })

				schema := arrow.NewSchema([]arrow.Field{
					{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
				}, nil)

				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(func() { rb.Release() })

				rb.Field(0).(*array.BooleanBuilder).Append(true)
				rb.Field(1).(*array.Float64Builder).Append(1.25)
				rb.Field(2).(*array.Int64Builder).Append(20)
				rb.Field(3).(*array.StringBuilder).Append("string")
				rb.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMilli()))

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
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildArrowData(ctx, tt.data)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
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

	makeSink := func() (*DuckLakeSink, *fakeDB, *fakeArrowViewManager, *int) {
		fdb := &fakeDB{}
		fav := &fakeArrowViewManager{}
		buildCalls := 0

		d := &DuckLakeSink{
			db:           fdb,
			arrowViewMgr: fav,
			buildArrowDataFn: func(ctx api.StreamContext, got map[string]any) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowData(ctx, got)
			},
		}
		_ = d.Provision(ctx, map[string]any{"table": "table"})
		return d, fdb, fav, &buildCalls
	}

	tests := []struct {
		name           string
		setup          func(d *DuckLakeSink, fdb *fakeDB, fav *fakeArrowViewManager, buildCalls *int)
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
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, buildCalls *int) {
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
			name: "error: arrowViewMgr RegisterRecordBatch fails",
			setup: func(_ *DuckLakeSink, _ *fakeDB, fav *fakeArrowViewManager, _ *int) {
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
			setup: func(_ *DuckLakeSink, fdb *fakeDB, _ *fakeArrowViewManager, _ *int) {
				fdb.errStr = "exec failed"
				fdb.numCorrectQueries = 1
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "db query execution failed",
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO table SELECT * FROM __ekuiper_ducklake_1"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name: "error: db not set",
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, _ *int) {
				s.db = nil
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "db not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: arrowViewMgr not set",
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, _ *int) {
				s.arrowViewMgr = nil
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
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, _ *int) {
				s.buildArrowDataFn = nil
			},
			data:           map[string]any{"t": int64(20)},
			wantErr:        "function build arrow data not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, fdb, fav, buildCalls := makeSink()
			if tt.setup != nil {
				tt.setup(s, fdb, fav, buildCalls)
			}

			err := s.Collect(ctx, testTuple{m: tt.data})

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.wantBuildCalls, *buildCalls)
			require.Equal(t, tt.wantDBQueries, fdb.queries)
			require.Equal(t, tt.wantViewCalls, fav.calls)
			require.Equal(t, tt.wantReleased, fav.released)
		})
	}
}

func TestInferArrowSchema(t *testing.T) {
	ctx, fl := newMockCtxWithFakeLogger("inferArrowSchema", "op")
	ts1, _ := time.Parse(time.RFC3339, "2026-03-23T10:15:30.000+02:00")
	// ts2 := ts1.Add(2 * time.Second)

	type tc struct {
		name            string
		row             map[string]any
		wantErr         string
		wantSchema      *arrow.Schema
		wantNil         bool
		wantLogContains []string
	}

	tests := []tc{
		{
			name: "happy path",
			row:  map[string]any{"string": "a", "float": 1.25, "integer": int64(20), "boolean": true, "time": ts1},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
			}, nil),
		},
		{
			name: "float32 -> float64",
			row:  map[string]any{"float": float32(1.25)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			}, nil),
		},
		{
			name: "float64 -> float64",
			row:  map[string]any{"float": float64(1.25)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			}, nil),
		},

		{
			name: "uint -> int64",
			row:  map[string]any{"integer": uint(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "uint8 -> int64",
			row:  map[string]any{"integer": uint8(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "uint16 -> int64",
			row:  map[string]any{"integer": uint16(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "uint32 -> int64",
			row:  map[string]any{"integer": uint32(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "uint64 -> int64",
			row:  map[string]any{"integer": uint64(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "int -> int64",
			row:  map[string]any{"integer": int(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "int8 -> int64",
			row:  map[string]any{"integer": int8(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "int16 -> int64",
			row:  map[string]any{"integer": int16(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "int32 -> int64",
			row:  map[string]any{"integer": int32(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name: "int64 -> int64",
			row:  map[string]any{"integer": int64(1)},
			wantSchema: arrow.NewSchema([]arrow.Field{
				{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
		},
		{
			name:            "null arrow data: empty value first row",
			row:             map[string]any{"t": nil},
			wantNil:         true,
			wantLogContains: []string{"empty inferred schema"},
		},
		{
			name:    "null arrow data: empty row",
			row:     nil,
			wantErr: "Ducklake sink: row is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fl.errorf = nil
			got, err := inferArrowSchemaFromRow(ctx, tt.row)

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, got)
				return
			}

			require.NoError(t, err)

			if len(tt.wantLogContains) == 0 {
				for _, want := range tt.wantLogContains {
					require.Contains(t, fl.errorf, want)
				}
			}
			if tt.wantNil {
				require.Nil(t, got)
				return
			}

			require.NotNil(t, got)
			require.True(t, tt.wantSchema.Equal(got.schema))

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
			name: "happy path",
			rows: []map[string]any{
				{"string": "a", "float": 1.25, "integer": int64(20), "boolean": true, "time": ts1},
				{"string": "b", "float": 2.5, "integer": int64(40), "boolean": false, "time": ts2},
			},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
					{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
					{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
				}, nil)
				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				// row 1
				rb.Field(0).(*array.BooleanBuilder).Append(true)
				rb.Field(1).(*array.Float64Builder).Append(1.25)
				rb.Field(2).(*array.Int64Builder).Append(20)
				rb.Field(3).(*array.StringBuilder).Append("a")
				rb.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(ts1.UnixMilli()))

				// row 2
				rb.Field(0).(*array.BooleanBuilder).Append(false)
				rb.Field(1).(*array.Float64Builder).Append(2.5)
				rb.Field(2).(*array.Int64Builder).Append(40)
				rb.Field(3).(*array.StringBuilder).Append("b")
				rb.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(ts2.UnixMilli()))

				rec := rb.NewRecordBatch()
				t.Cleanup(rec.Release)
				return rec
			},
		},
		{
			name: "float32 -> float64",
			rows: []map[string]any{
				{"float": float32(1.25)},
				{"float": float32(2.5)},
			},
			wantRec: func(t *testing.T) arrow.RecordBatch {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				t.Cleanup(func() { mem.AssertSize(t, 0) })
				schema := arrow.NewSchema([]arrow.Field{
					{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
				}, nil)
				rb := array.NewRecordBuilder(mem, schema)
				t.Cleanup(rb.Release)

				// row 1
				rb.Field(0).(*array.Float64Builder).Append(1.25)

				// row 2
				rb.Field(0).(*array.Float64Builder).Append(2.5)

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
			name: "integer type mismatch vs first row",
			rows: []map[string]any{
				{"integer": int64(1)},
				{"integer": "string"},
			},
			wantErr: "type mismatch",
		},
		{
			name: "float type mismatch vs first row",
			rows: []map[string]any{
				{"float": float64(1)},
				{"float": "string"},
			},
			wantErr: "type mismatch",
		},
		{
			name: "bool type mismatch vs first row",
			rows: []map[string]any{
				{"bool": true},
				{"bool": "string"},
			},
			wantErr: "type mismatch",
		},
		{
			name: "string type mismatch vs first row",
			rows: []map[string]any{
				{"string": "string"},
				{"string": true},
			},
			wantErr: "type mismatch",
		},
		{
			name: "timestamp type mismatch vs first row",
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
			wantLogContains: []string{"empty data"},
		},
		{
			name:            "null arrow data: empty first row",
			rows:            []map[string]any{{"a": nil}},
			wantNil:         true,
			wantLogContains: []string{"empty first row"},
		},
		{
			name:            "null arrow data: empty value first row",
			rows:            []map[string]any{{"t": nil}, {"t": int64(40)}},
			wantNil:         true,
			wantLogContains: []string{"empty inferred schema"},
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
			wantLogContains: []string{"empty value Row <1> Field <t>"},
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
			wantLogContains: []string{"empty value inferring schema, field <b>"},
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

			if len(tt.wantLogContains) == 0 {
				for _, want := range tt.wantLogContains {
					require.Contains(t, fl.errorf, want)
				}
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

	makeSink := func() (*DuckLakeSink, *fakeDB, *fakeArrowViewManager, *int) {
		fdb := &fakeDB{}
		fav := &fakeArrowViewManager{}
		buildCalls := 0

		d := &DuckLakeSink{
			db:           fdb,
			arrowViewMgr: fav,
			buildArrowDataListFn: func(ctx api.StreamContext, got []map[string]any) (arrow.RecordBatch, error) {
				buildCalls++
				return buildArrowDataList(ctx, got)
			},
		}
		_ = d.Provision(ctx, map[string]any{"table": "table"})
		return d, fdb, fav, &buildCalls
	}

	tests := []struct {
		name           string
		setup          func(d *DuckLakeSink, fdb *fakeDB, fav *fakeArrowViewManager, buildCalls *int)
		data           []map[string]any
		wantErr        string
		wantBuildCalls int
		wantDBQueries  []string
		wantViewCalls  int
		wantReleased   bool
		// wantLogContains string
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
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, buildCalls *int) {
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
			name: "error: arrowViewMgr RegisterRecordBatch fails",
			setup: func(_ *DuckLakeSink, _ *fakeDB, fav *fakeArrowViewManager, _ *int) {
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
			setup: func(_ *DuckLakeSink, fdb *fakeDB, _ *fakeArrowViewManager, _ *int) {
				fdb.errStr = "exec failed"
				fdb.numCorrectQueries = 1
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "db query execution failed",
			wantBuildCalls: 1,
			wantDBQueries:  []string{"INSERT INTO table SELECT * FROM __ekuiper_ducklake_1"},
			wantViewCalls:  1,
			wantReleased:   true,
		},
		{
			name: "error: db not set",
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, _ *int) {
				s.db = nil
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "db not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
		{
			name: "error: arrowViewMgr not set",
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, _ *int) {
				s.arrowViewMgr = nil
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
			setup: func(s *DuckLakeSink, _ *fakeDB, _ *fakeArrowViewManager, _ *int) {
				s.buildArrowDataListFn = nil
			},
			data:           []map[string]any{{"t": int64(20)}, {"t": int64(40)}},
			wantErr:        "function build arrow data not set",
			wantBuildCalls: 0,
			wantDBQueries:  nil,
			wantViewCalls:  0,
			wantReleased:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, fdb, fav, buildCalls := makeSink()
			if tt.setup != nil {
				tt.setup(s, fdb, fav, buildCalls)
			}

			err := s.CollectList(ctx, testTupleList{ms: tt.data})

			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}

			// if tt.wantLogContains != "" {
			// 	require.Contains(t, fl.errorf, tt.wantLogContains)
			// }

			require.Equal(t, tt.wantBuildCalls, *buildCalls)
			require.Equal(t, tt.wantDBQueries, fdb.queries)
			require.Equal(t, tt.wantViewCalls, fav.calls)
			require.Equal(t, tt.wantReleased, fav.released)
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name    string
		in      any
		want    int64
		wantErr string
	}{
		{name: "int", in: int(42), want: 42},
		{name: "int8", in: int8(42), want: 42},
		{name: "int16", in: int16(42), want: 42},
		{name: "int32", in: int32(42), want: 42},
		{name: "int64", in: int64(42), want: 42},
		{name: "uint", in: uint(42), want: 42},
		{name: "uint8", in: uint8(42), want: 42},
		{name: "uint16", in: uint16(42), want: 42},
		{name: "uint32", in: uint32(42), want: 42},
		{name: "uint64", in: uint64(42), want: 42},
		{name: "float64 is error", in: float64(1.2), wantErr: "expected int"},
		{name: "string is error", in: "1", wantErr: "expected int"},
		{name: "bool is error", in: true, wantErr: "expected int"},
		{name: "time is error", in: time.UnixMilli(0), wantErr: "expected int"},
		{name: "nil is error", in: nil, wantErr: "expected int"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toInt64(tt.in)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}
