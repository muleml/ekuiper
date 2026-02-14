package ducklake

import (
	"fmt"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

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
