package ducklake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type sqlEngine interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Close() error
}

type CatalogConf struct {
	Type     string `json:"catalog_type"`
	Host     string `json:"catalog_host"`
	Port     int    `json:"catalog_port"`
	Database string `json:"catalog_database"`
	User     string `json:"catalog_user"`
	Password string `json:"catalog_password"`
}

type StorageConf struct {
	Type     string `json:"storage_type"`
	Endpoint string `json:"storage_endpoint"`
	Bucket   string `json:"storage_bucket"`
	KeyId    string `json:"storage_key_id"`
	Secret   string `json:"storage_secret"`
}

type c struct {
	Table string `json:"table"`

	Storage StorageConf
	Catalog CatalogConf
}

type DuckLakeSink struct {
	conf c
	db   sqlEngine
}

func (s *DuckLakeSink) Provision(ctx api.StreamContext, props map[string]any) error {

	s.conf = c{
		Catalog: CatalogConf{
			Type: "duckdb",
		},
	}

	if err := cast.MapToStruct(props, &s.conf); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
	}
	if s.conf.Table == "" {
		return fmt.Errorf("error configuring ducklake sink: missing table name")
	}

	if err := cast.MapToStruct(props, &s.conf.Catalog); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
	}

	switch s.conf.Catalog.Type {
	case "duckdb":
	case "postgres":
		if s.conf.Catalog.Host == "" {
			return fmt.Errorf("error configuring ducklake sink: host is required for postgres")
		}
		if s.conf.Catalog.Database == "" {
			return fmt.Errorf("error configuring ducklake sink: database name is required for postgres")
		}
	default:
		return fmt.Errorf("error configuring ducklake sink: catalog not supported")
	}

	if err := cast.MapToStruct(props, &s.conf.Storage); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
	}

	if s.conf.Storage.Type == "" {
		return fmt.Errorf("error configuring ducklake sink: missing storage")
	}

	switch s.conf.Storage.Type {
	case "s3":
		if s.conf.Storage.Endpoint == "" {
			return fmt.Errorf("error configuring ducklake sink: missing storage s3 endpoint")
		}
		if s.conf.Storage.Bucket == "" {
			return fmt.Errorf("error configuring ducklake sink: missing storage s3 bucket")
		}
	default:
		return fmt.Errorf("error configuring ducklake sink: storage type not supported")
	}

	ctx.GetLogger().Infof("ducklake sink provision successfully terminated")

	return nil
}

func (m *DuckLakeSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnecting, "")

	query := "INSTALL ducklake;"
	_, err := m.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	if m.conf.Catalog.Type == "postgres" {
		query := "INSTALL postgres;"
		_, err = m.db.ExecContext(ctx, query)
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
			return fmt.Errorf("Ducklake sink connection error: %s", err)
		}
	}

	query, _ = queryCreateStorageSecret(m.conf.Storage)
	_, err = m.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	query, _ = queryCreateCatalogSecret(m.conf.Catalog)
	if query != "" {
		_, err = m.db.ExecContext(ctx, query)
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
			return fmt.Errorf("Ducklake sink connection error: %s", err)
		}
	}

	query, _ = queryAttachDucklake(m.conf.Storage, m.conf.Catalog.Type)
	_, err = m.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	query = "USE the_ducklake;"
	_, err = m.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	sch(api.ConnectionConnected, "")
	ctx.GetLogger().Infof("ducklake sink succesfully connected")
	return nil
}

func (m *DuckLakeSink) Close(ctx api.StreamContext) error {
	if m.db == nil {
		return fmt.Errorf("error closing ducklake sink: no db to close")
	}
	err := m.db.Close()
	if err != nil {
		return fmt.Errorf("error closing ducklake sink: %s", err)
	}
	return nil
}

func queryCreateStorageSecret(conf StorageConf) (string, error) {
	switch conf.Type {
	case "s3":
		query := fmt.Sprintf("CREATE OR REPLACE s3_secret (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s')", conf.KeyId, conf.Secret, conf.Endpoint)
		return query, nil
	default:
		return "", fmt.Errorf("error connecting ducklake sink: storage type not supported")
	}
}

func queryCreateCatalogSecret(conf CatalogConf) (string, error) {
	switch conf.Type {
	case "duckdb":
		return "", nil
	case "postgres":
		query := fmt.Sprintf("CREATE OR REPLACE postgres_secret (TYPE postgres, HOST '%s', PORT %d, DATABASE %s, USER '%s', PASSWORD '%s')", conf.Host, conf.Port, conf.Database, conf.User, conf.Password)
		return query, nil
	default:
		return "", fmt.Errorf("error connecting ducklake sink: catalog type not supported")
	}
}

func queryAttachDucklake(conf StorageConf, catalogType string) (string, error) {
	secret, _ := getSecret(catalogType)
	switch conf.Type {
	case "s3":
		query := fmt.Sprintf("ATTACH 'ducklake:%s' AS the_ducklake (DATA_PATH 's3://%s');", secret, conf.Bucket)
		return query, nil
	default:
		return "", fmt.Errorf("error connecting ducklake sink: storage type not supported")
	}
}

func getSecret(catalogType string) (string, error) {
	switch catalogType {
	case "duckdb":
		return "", nil
	case "postgres":
		return "postgres_secret", nil
	default:
		return "", fmt.Errorf("error connecting ducklake sink: catalog type not supported")
	}
}
