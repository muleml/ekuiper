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
				Table: "table",
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
				Table: "table",
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
				Table: "table",
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

func TestBuildArrowData(t *testing.T) {
	ts, _ := time.Parse(time.RFC3339, "2026-03-23T10:15:30.000+02:00")
	data := map[string]any{
		"string":  "string",
		"float":   1.25,
		"integer": int64(20),
		"boolean": true,
		"time":    ts,
	}

	// create arrow array for comparison
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "boolean", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "float", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "integer", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "string", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "time", Type: arrow.FixedWidthTypes.Timestamp_ms, Nullable: true},
	}, nil)

	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	rb.Field(0).(*array.BooleanBuilder).Append(true)
	rb.Field(1).(*array.Float64Builder).Append(1.25)
	rb.Field(2).(*array.Int64Builder).Append(20)
	rb.Field(3).(*array.StringBuilder).Append("string")
	rb.Field(4).(*array.TimestampBuilder).Append(arrow.Timestamp(ts.UnixMilli()))

	wantsRec := rb.NewRecordBatch()
	defer wantsRec.Release()

	arrowData, err := buildArrowData(data)
	require.NoError(t, err)

	require.Len(t, data, 5)
	require.True(t, array.RecordEqual(wantsRec, arrowData))
}

// func TestInternalCollect_HappyPath(t *testing.T) {
// 	ctx := mockContext.NewMockContext("collect_ok", "op")
//
// 	fdb := &fakeDB{}
// 	s := &DuckLakeSink{db: fdb}
//
// 	data := map[string]any{"t": 20}
//
// 	err := s.collect(ctx, data)
// 	require.NoError(t, err)
//
// 	require.Equal(t, 1, len(fdb.queries))
// }

// func TestInternalCollect_ReturnsErrorIfNotPresent(t *testing.T) {
// 	ctx := mockContext.NewMockContext("collect_not_present", "op")
//
// 	s := newCollectSink(nil)
//
// 	data := map[string]any{"temperature": 20}
// 	err := s.collect(ctx, data)
// 	require.ErrorContains(t, err, "client not selected")
// }
//
// func TestInternalCollect_PropagatesWritePointsErrorAsIOError(t *testing.T) {
// 	timex.Set(10)
// 	ctx := mockContext.NewMockContext("collect_write_err", "op")
//
// 	fc := &fakeInflux3Client{writeErr: errors.New("boom")}
// 	s := newCollectSink(fc)
//
// 	data := map[string]any{"temperature": 20}
// 	err := s.collect(ctx, data)
// 	require.Error(t, err)
// 	require.True(t, errorx.IsIOError(err))
// 	require.Contains(t, err.Error(), "boom")
// }
//
// func TestInternalCollect_TransformPointsError_DoesNotWrite(t *testing.T) {
// 	ctx := mockContext.NewMockContext("collect_transform_err", "op")
//
// 	fc := &fakeInflux3Client{}
// 	s := newCollectSink(fc)
//
// 	err := s.collect(ctx, []byte{1, 2, 3})
// 	require.Error(t, err)
// 	require.Equal(t, "sink needs map or []map, but receive unsupported data [1 2 3]", err.Error())
// 	require.Equal(t, 0, fc.writeCalls)
// }
//
// func TestCollect(t *testing.T) {
// 	timex.Set(10)
// 	ctx := mockContext.NewMockContext("collect_ok", "op")
//
// 	fc := &fakeInflux3Client{}
// 	s := newCollectSink(fc)
//
// 	item := testTuple{m: map[string]any{"t": 20}}
// 	wantsPt := []*influxdb3.Point{
// 		influxdb3.NewPoint("m",
// 			map[string]string{"tag": "v"},
// 			map[string]any{"t": 20},
// 			time.UnixMilli(10),
// 		),
// 	}
//
// 	err := s.Collect(ctx, item)
// 	require.NoError(t, err)
//
// 	require.Equal(t, 1, fc.writeCalls)
// 	require.Len(t, fc.lastPoints, 1)
// 	require.Equal(t, wantsPt[0].Values, fc.lastPoints[0].Values)
// }
//
// func TestCollectList(t *testing.T) {
// 	timex.Set(10)
// 	ctx := mockContext.NewMockContext("collect_ok", "op")
//
// 	fc := &fakeInflux3Client{}
// 	s := newCollectSink(fc)
//
// 	items := testTupleList{ms: []map[string]any{
// 		{"t": 20},
// 		{"t": 40},
// 	}}
// 	wantsPt := []*influxdb3.Point{
// 		influxdb3.NewPoint("m",
// 			map[string]string{"tag": "v"},
// 			map[string]any{"t": 20},
// 			time.UnixMilli(10),
// 		),
// 		influxdb3.NewPoint("m",
// 			map[string]string{"tag": "v"},
// 			map[string]any{"t": 40},
// 			time.UnixMilli(10),
// 		),
// 	}
//
// 	err := s.CollectList(ctx, items)
// 	require.NoError(t, err)
//
// 	require.Equal(t, 1, fc.writeCalls)
// 	require.Len(t, fc.lastPoints, 2)
// 	require.Equal(t, wantsPt[0].Values, fc.lastPoints[0].Values)
// 	require.Equal(t, wantsPt[1].Values, fc.lastPoints[1].Values)
// }
