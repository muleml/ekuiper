package ducklake

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type sqlEngine interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Close() error
}

type arrowViewManager interface {
	RegisterRecordBatch(ctx context.Context, name string, batch arrow.RecordBatch) (release func(), err error)
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
	conf             c
	db               sqlEngine
	arrowViewMgr     arrowViewManager
	buildArrowDataFn func(map[string]any) (arrow.RecordBatch, error)
	viewSeq          uint64
}

func (d *DuckLakeSink) Provision(ctx api.StreamContext, props map[string]any) error {

	d.conf = c{
		Catalog: CatalogConf{
			Type: "duckdb",
		},
	}

	if err := cast.MapToStruct(props, &d.conf); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
	}
	if d.conf.Table == "" {
		return fmt.Errorf("error configuring ducklake sink: missing table name")
	}

	if err := cast.MapToStruct(props, &d.conf.Catalog); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
	}

	switch d.conf.Catalog.Type {
	case "duckdb":
	case "postgres":
		if d.conf.Catalog.Host == "" {
			return fmt.Errorf("error configuring ducklake sink: host is required for postgres")
		}
		if d.conf.Catalog.Database == "" {
			return fmt.Errorf("error configuring ducklake sink: database name is required for postgres")
		}
	default:
		return fmt.Errorf("error configuring ducklake sink: catalog not supported")
	}

	if err := cast.MapToStruct(props, &d.conf.Storage); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
	}

	if d.conf.Storage.Type == "" {
		return fmt.Errorf("error configuring ducklake sink: missing storage")
	}

	switch d.conf.Storage.Type {
	case "s3":
		if d.conf.Storage.Endpoint == "" {
			return fmt.Errorf("error configuring ducklake sink: missing storage s3 endpoint")
		}
		if d.conf.Storage.Bucket == "" {
			return fmt.Errorf("error configuring ducklake sink: missing storage s3 bucket")
		}
	default:
		return fmt.Errorf("error configuring ducklake sink: storage type not supported")
	}

	ctx.GetLogger().Infof("ducklake sink provision successfully terminated")

	return nil
}

func (d *DuckLakeSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnecting, "")

	query := "INSTALL ducklake;"
	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	if d.conf.Catalog.Type == "postgres" {
		query := "INSTALL postgres;"
		_, err = d.db.ExecContext(ctx, query)
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
			return fmt.Errorf("Ducklake sink connection error: %s", err)
		}
	}

	query, _ = queryCreateStorageSecret(d.conf.Storage)
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	query, _ = queryCreateCatalogSecret(d.conf.Catalog)
	if query != "" {
		_, err = d.db.ExecContext(ctx, query)
		if err != nil {
			sch(api.ConnectionDisconnected, err.Error())
			return fmt.Errorf("Ducklake sink connection error: %s", err)
		}
	}

	query, _ = queryAttachDucklake(d.conf.Storage, d.conf.Catalog.Type)
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	query = "USE the_ducklake;"
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		sch(api.ConnectionDisconnected, err.Error())
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}

	sch(api.ConnectionConnected, "")
	ctx.GetLogger().Infof("ducklake sink successfully connected")
	return nil
}

func (d *DuckLakeSink) Close(ctx api.StreamContext) error {
	if d.db == nil {
		return fmt.Errorf("error closing ducklake sink: no db to close")
	}
	err := d.db.Close()
	if err != nil {
		return fmt.Errorf("error closing ducklake sink: %s", err)
	}
	return nil
}

func (d *DuckLakeSink) Ping(ctx api.StreamContext, props map[string]any) error {
	query := "INSTALL ducklake;"
	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}

	if d.conf.Catalog.Type == "postgres" {
		query := "INSTALL postgres;"
		_, err = d.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("Ducklake sink ping connection error: %s", err)
		}
	}

	query, _ = queryCreateStorageSecret(d.conf.Storage)
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}

	query, _ = queryCreateCatalogSecret(d.conf.Catalog)
	if query != "" {
		_, err = d.db.ExecContext(ctx, query)
		if err != nil {
			return fmt.Errorf("Ducklake sink ping connection error: %s", err)
		}
	}

	query, _ = queryAttachDucklake(d.conf.Storage, d.conf.Catalog.Type)
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}

	query = "USE memory;"
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}
	query = "DETACH the_ducklake;"
	_, err = d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}

	ctx.GetLogger().Infof("ducklake sink successful ping")
	return nil
}

func (d *DuckLakeSink) collect(ctx api.StreamContext, data map[string]any) error {
	// if d.db == nil {
	// 	return fmt.Errorf("Ducklake sink collect error: db not found")
	// }
	// if d.arrowViewMgr == nil {
	// 	return fmt.Errorf("Ducklake sink collect error: arrow view manager not found")
	// }
	arrowData, _ := d.buildArrowDataFn(data)
	defer arrowData.Release()
	viewName := fmt.Sprintf("__ekuiper_ducklake_%d", atomic.AddUint64(&d.viewSeq, 1))

	release, _ := d.arrowViewMgr.RegisterRecordBatch(context.Background(), viewName, arrowData)
	defer release()
	qt, _ := quoteIdent(d.conf.Table)
	qv, _ := quoteIdent(viewName)
	query := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s", qt, qv)
	_, _ = d.db.ExecContext(ctx, query)
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

func buildArrowData(data map[string]any) (arrow.RecordBatch, error) {
	if len(data) < 1 {
		return nil, nil
	}
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	// alphabetically order keys
	sort.Strings(keys)

	fields := make([]arrow.Field, len(keys))
	dts := make([]arrow.DataType, len(keys))
	for i, k := range keys {
		v := data[k]
		if v == nil {
			return nil, fmt.Errorf("Ducklake sink error creating arrow data: null value in field %s", k)
		}
		var dt arrow.DataType
		switch v.(type) {
		case string:
			dt = arrow.BinaryTypes.String
		case bool:
			dt = arrow.FixedWidthTypes.Boolean
		case float32, float64:
			dt = arrow.PrimitiveTypes.Float64
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			dt = arrow.PrimitiveTypes.Int64
		case time.Time:
			dt = arrow.FixedWidthTypes.Timestamp_ms
		default:
			return nil, fmt.Errorf("Ducklake sink error creating arrow data: field <%s> has unsupported type <%T>", k, v)
		}
		fields[i] = arrow.Field{Name: k, Type: dt, Nullable: true}
		dts[i] = dt
	}
	schema := arrow.NewSchema(fields, nil)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()

	for i, k := range keys {
		v := data[k]
		switch dts[i].ID() {
		case arrow.STRING:
			rb.Field(i).(*array.StringBuilder).Append(v.(string))
		case arrow.BOOL:
			rb.Field(i).(*array.BooleanBuilder).Append(v.(bool))
		case arrow.FLOAT64:
			switch x := v.(type) {
			case float32:
				rb.Field(i).(*array.Float64Builder).Append(float64(x))
			case float64:
				rb.Field(i).(*array.Float64Builder).Append(x)
			}
		case arrow.INT64:
			x, err := toInt64(v)
			if err != nil {
				return nil, fmt.Errorf("%s - Field <%s>", err, v)
			}
			rb.Field(i).(*array.Int64Builder).Append(x)
		case arrow.TIMESTAMP:
			t := v.(time.Time)
			rb.Field(i).(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMilli())) // use unix absolute time
		default:
			return nil, fmt.Errorf("Ducklake sink error creating arrow data: field <%q> unexpected arrow type <%s>", k, dts[i])
		}
	}
	return rb.NewRecordBatch(), nil
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int64:
		return x, nil
	case uint:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	default:
		return 0, fmt.Errorf("Ducklake sink error creating arrow data: expected int in got %T", v)
	}
}

// func validateIdentLoose(s string) error {
// 	if s == "" {
// 		return fmt.Errorf("identifier is empty")
// 	}
// 	if strings.Contains(s, ";") {
// 		return fmt.Errorf("identifier contains ';'")
// 	}
// 	for _, r := range s {
// 		// blocca caratteri di controllo (0x00-0x1F e 0x7F)
// 		if r < 0x20 || r == 0x7f {
// 			return fmt.Errorf("identifier contains control char")
// 		}
// 	}
// 	return nil
// }
// func quoteIdentDuckDB(ident string) (string, error) {
// 	if err := validateIdentLoose(ident); err != nil {
// 		return "", err
// 	}
// 	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`, nil
// }

func validateIdentLoose(s string) error {
	return nil
}

func quoteIdent(ident string) (string, error) {
	return ident, nil
}
