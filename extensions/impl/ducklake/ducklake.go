package ducklake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// Interfaces
type sqlEngine interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	Close() error
}

type sqlPool interface {
	Conn(ctx context.Context) (*sql.Conn, error)
	Close() error
}

type arrowManager interface {
	RegisterRecordBatch(name string, batch arrow.RecordBatch) (release func(), err error)
}

// Ducklake sink
type inferredSchema struct {
	keys   []string
	schema *arrow.Schema
	dts    []arrow.DataType
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
	UseSSL   bool   `json:"use_ssl"`
	UrlStyle string `json:"url_style"`
}

type c struct {
	Table          string `json:"table"`
	sanitizedTable string

	Storage StorageConf
	Catalog CatalogConf
}

type DuckLakeSink struct {
	conf                 c
	db                   sqlPool
	conn                 sqlEngine
	arrowMgr             arrowManager
	buildArrowDataFn     func(api.StreamContext, map[string]any) (arrow.RecordBatch, error)
	buildArrowDataListFn func(api.StreamContext, []map[string]any) (arrow.RecordBatch, error)
	viewSeq              uint64
	ducklakeName         string
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
	if err := validateSQLString(d.conf.Table); err != nil {
		return fmt.Errorf("error configuring ducklake sink: invalid table name %s", d.conf.Table)
	}
	// input sanitizer
	d.conf.sanitizedTable = strings.ReplaceAll(d.conf.Table, `"`, ``)
	d.conf.sanitizedTable = strings.ReplaceAll(d.conf.sanitizedTable, ` `, ``)

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

	d.ducklakeName = "the_ducklake"

	ctx.GetLogger().Infof("ducklake sink provision successfully terminated")

	return nil
}

func (d *DuckLakeSink) Connect(ctx api.StreamContext, sch api.StatusChangeHandler) error {
	sch(api.ConnectionConnecting, "")

	if d.db == nil {
		err := d.setupDuckdb("")
		if err != nil {
			return err
		}
	}
	if d.arrowMgr == nil && d.conn == nil {
		err := d.setupArrowManager(ctx)
		if err != nil {
			return err
		}
	}

	err := d.attachDucklake(ctx)
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
	if d.conn == nil {
		return fmt.Errorf("error closing ducklake sink: no conn to close")
	}
	err = d.conn.Close()
	if err != nil {
		return fmt.Errorf("error closing ducklake sink: %s", err)
	}
	return nil
}

func (d *DuckLakeSink) Ping(ctx api.StreamContext, props map[string]any) error {
	if d.db == nil {
		err := d.setupDuckdb("")
		if err != nil {
			return err
		}
	}
	if d.arrowMgr == nil && d.conn == nil {
		err := d.setupArrowManager(ctx)
		if err != nil {
			_ = d.Close(ctx)
			return err
		}
	}
	err := d.attachDucklake(ctx)
	if err != nil {
		_ = d.Close(ctx)
		return fmt.Errorf("Ducklake sink connection error: %s", err)
	}
	query := "USE memory;"
	_, err = d.conn.ExecContext(ctx, query)
	if err != nil {
		_ = d.Close(ctx)
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}
	query = "DETACH the_ducklake;"
	_, err = d.conn.ExecContext(ctx, query)
	if err != nil {
		_ = d.Close(ctx)
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}
	err = d.Close(ctx)
	if err != nil {
		return fmt.Errorf("Ducklake sink ping connection error: %s", err)
	}

	ctx.GetLogger().Infof("ducklake sink successful ping")
	return nil
}

func (d *DuckLakeSink) Collect(ctx api.StreamContext, item api.MessageTuple) error {
	data := item.ToMap()
	err := d.checkSetup()
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error: %s", err)
	}
	arrowData, err := d.buildArrowDataFn(ctx, data)
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error - arrow build failed: %s", err)
	}
	if arrowData == nil {
		return nil
	}
	defer arrowData.Release()
	return d.insertRecordBatch(ctx, arrowData)
}

func (d *DuckLakeSink) CollectList(ctx api.StreamContext, items api.MessageTupleList) error {
	data := items.ToMaps()
	err := d.checkSetup()
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error: %s", err)
	}
	arrowData, err := d.buildArrowDataListFn(ctx, data)
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error - arrow build failed: %s", err)
	}
	if arrowData == nil {
		return nil
	}
	defer arrowData.Release()
	return d.insertRecordBatch(ctx, arrowData)
}

func (d *DuckLakeSink) insertRecordBatch(ctx api.StreamContext, batch arrow.RecordBatch) error {
	viewName := fmt.Sprintf("__ekuiper_ducklake_%d", atomic.AddUint64(&d.viewSeq, 1))

	release, err := d.arrowMgr.RegisterRecordBatch(viewName, batch)
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error - arrow register view failed: %s", err)
	}
	defer release()
	query, err := d.getInsertQuery(batch.Schema(), viewName)
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error - db query creation failed: %s", err)
	}
	_, err = d.conn.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("Ducklake sink collect error - db query execution failed: %s", err)
	}
	return nil
}

func (d *DuckLakeSink) getInsertQuery(schema *arrow.Schema, viewName string) (string, error) {
	if len(schema.Fields()) == 0 {
		return "", fmt.Errorf("empty arrow schema")
	}
	columns := ""
	for i, field := range schema.Fields() {
		if i > 0 {
			columns += ", "
		}
		columns += field.Name
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM %s;", d.ducklakeName, d.conf.sanitizedTable, columns, columns, viewName)
	return query, nil
}

func (d *DuckLakeSink) checkSetup() error {
	if d.conn == nil {
		return fmt.Errorf("conn not set")
	}
	if d.arrowMgr == nil {
		return fmt.Errorf("arrow view manager not set")
	}
	if d.buildArrowDataFn == nil {
		return fmt.Errorf("function build arrow data not set")
	}
	if d.buildArrowDataListFn == nil {
		return fmt.Errorf("function build arrow data list not set")
	}
	return nil
}

func (d *DuckLakeSink) setupDuckdb(dsn string) error {
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return err
	}
	d.db = db
	return nil
}

func (d *DuckLakeSink) setupArrowManager(ctx api.StreamContext) error {
	if d.db == nil {
		return fmt.Errorf("conn not set")
	}
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return err
	}
	mgr, err := newArrowManager(conn)
	if err != nil {
		return err
	}
	d.conn = conn
	d.arrowMgr = mgr
	return nil
}

func (d *DuckLakeSink) Info() model.SinkInfo {
	return model.SinkInfo{HasBatch: true}
}

func (d *DuckLakeSink) attachDucklake(ctx context.Context) error {
	query := "INSTALL ducklake;"
	_, err := d.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	if d.conf.Catalog.Type == "postgres" {
		query := "INSTALL postgres;"
		_, err = d.conn.ExecContext(ctx, query)
		if err != nil {
			return err
		}
	}

	query, _ = queryCreateStorageSecret(d.conf.Storage)
	_, err = d.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	query, _ = queryCreateCatalogSecret(d.conf.Catalog)
	if query != "" {
		_, err = d.conn.ExecContext(ctx, query)
		if err != nil {
			return err
		}
	}

	query, _ = queryCreateSecretDucklake(d.conf.Storage, d.conf.Catalog.Type)
	_, err = d.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf("ATTACH 'ducklake:ducklake_secret' AS %s;", d.ducklakeName)
	_, err = d.conn.ExecContext(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func queryCreateStorageSecret(conf StorageConf) (string, error) {
	switch conf.Type {
	case "s3":
		useSSL := "FALSE"
		if conf.UseSSL {
			useSSL = "TRUE"
		}
		urlStyle := "path"
		if conf.UrlStyle != "" {
			urlStyle = conf.UrlStyle
		}
		query := fmt.Sprintf("CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', USE_SSL %s, URL_STYLE '%s');", conf.KeyId, conf.Secret, conf.Endpoint, useSSL, urlStyle)
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
		query := fmt.Sprintf("CREATE OR REPLACE SECRET postgres_secret (TYPE postgres, HOST '%s', PORT %d, DATABASE %s, USER '%s', PASSWORD '%s');", conf.Host, conf.Port, conf.Database, conf.User, conf.Password)
		return query, nil
	default:
		return "", fmt.Errorf("error connecting ducklake sink: catalog type not supported")
	}
}

func queryCreateSecretDucklake(conf StorageConf, catalogType string) (string, error) {
	var metaDataPath string
	var metaDataParametersMap string
	switch catalogType {
	case "duckdb":
		metaDataPath = "metadata.duckdb"
		metaDataParametersMap = ""
	case "postgres":
		metaDataPath = ""
		metaDataParametersMap = "'TYPE': 'postgres', 'SECRET': 'postgres_secret'"
	default:
		return "", fmt.Errorf("error connecting ducklake sink: catalog type not supported")
	}
	var dataPath string
	switch conf.Type {
	case "s3":
		dataPath = "s3://ducklake"
	default:
		return "", fmt.Errorf("error connecting ducklake sink: storage type not supported")
	}

	secret := fmt.Sprintf("CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '%s', DATA_PATH '%s', METADATA_PARAMETERS MAP {%s});", metaDataPath, dataPath, metaDataParametersMap)
	return secret, nil
}

func buildArrowData(ctx api.StreamContext, data map[string]any) (arrow.RecordBatch, error) {
	if len(data) < 1 {
		ctx.GetLogger().Errorf("Ducklake sink: empty data")
		return nil, nil
	}

	arrowInferredSchema, err := inferArrowSchemaFromRow(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("Ducklake sink error creating arrow data: %s", err)
	}
	if arrowInferredSchema == nil {
		return nil, nil
	}
	rb := array.NewRecordBuilder(memory.DefaultAllocator, arrowInferredSchema.schema)
	defer rb.Release()

	if err := appendRow(ctx, rb, arrowInferredSchema, data, 0, true); err != nil {
		return nil, fmt.Errorf("Ducklake sink error creating arrow data: %s - ", err)
	}
	return rb.NewRecordBatch(), nil
}

func buildArrowDataList(ctx api.StreamContext, data []map[string]any) (arrow.RecordBatch, error) {
	if len(data) < 1 {
		ctx.GetLogger().Errorf("Ducklake sink: empty data")
		return nil, nil
	}
	if len(data[0]) == 0 {
		ctx.GetLogger().Errorf("Ducklake sink: empty first row")
		return nil, nil
	}

	arrowInferredSchema, err := inferArrowSchemaFromRow(ctx, data[0])
	if err != nil {
		return nil, fmt.Errorf("Ducklake sink error creating arrow data: %s", err)
	}
	if arrowInferredSchema != nil {
		rb := array.NewRecordBuilder(memory.DefaultAllocator, arrowInferredSchema.schema)
		defer rb.Release()

		for rowIdx, row := range data {
			if err := appendRow(ctx, rb, arrowInferredSchema, row, rowIdx, true); err != nil {
				return nil, fmt.Errorf("Ducklake sink error creating arrow data: %s - Row <%d>", err, rowIdx)
			}
		}
		return rb.NewRecordBatch(), nil
	}
	return nil, nil
}

func inferArrowSchemaFromRow(ctx api.StreamContext, row map[string]any) (*inferredSchema, error) {
	if len(row) == 0 {
		return nil, fmt.Errorf("Ducklake sink: row is empty")
	}

	keys := make([]string, 0, len(row))
	for k := range row {
		if row[k] != nil {
			keys = append(keys, k)
		} else {
			ctx.GetLogger().Errorf("Ducklake sink: empty value inferring schema, field <%s>", k)
		}
	}
	if len(keys) == 0 {
		ctx.GetLogger().Errorf("Ducklake sink: empty inferred schema")
		return nil, nil
	}
	sort.Strings(keys)

	fields := make([]arrow.Field, len(keys))
	dts := make([]arrow.DataType, len(keys))
	for i, k := range keys {
		v := row[k]
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
			return nil, fmt.Errorf("field <%s> has unsupported type <%T>", k, v)
		}
		fields[i] = arrow.Field{Name: k, Type: dt, Nullable: true}
		dts[i] = dt
	}
	return &inferredSchema{
		keys:   keys,
		schema: arrow.NewSchema(fields, nil),
		dts:    dts,
	}, nil
}

func appendValueToField(b array.Builder, dt arrow.DataType, v any) error {
	switch dt.ID() {
	case arrow.STRING:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("type mismatch, expected string")
		}
		b.(*array.StringBuilder).Append(s)
	case arrow.BOOL:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("type mismatch, expected bool")
		}
		b.(*array.BooleanBuilder).Append(x)
	case arrow.FLOAT64:
		switch x := v.(type) {
		case float32:
			b.(*array.Float64Builder).Append(float64(x))
		case float64:
			b.(*array.Float64Builder).Append(x)
		default:
			return fmt.Errorf("type mismatch, expected float")
		}
	case arrow.INT64:
		x, err := toInt64(v)
		if err != nil {
			return err
		}
		b.(*array.Int64Builder).Append(x)
	case arrow.TIMESTAMP:
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("type mismatch, expected timestamp")
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMilli()))
	default:
		return fmt.Errorf("type mismatch, unexpected arrow type")
	}
	return nil
}

func appendRow(ctx api.StreamContext, rb *array.RecordBuilder, schema *inferredSchema, row map[string]any, rowNum int, allowNulls bool) error {
	for colIdx, k := range schema.keys {
		v, ok := row[k]
		if !ok || v == nil {
			if allowNulls {
				ctx.GetLogger().Errorf("Ducklake sink: empty value Row <%d> Field <%s>", rowNum, k)
				rb.Field(colIdx).AppendNull()
				continue
			}
		}
		if err := appendValueToField(rb.Field(colIdx), schema.dts[colIdx], v); err != nil {
			return fmt.Errorf("Field <%s> Value <%s> - %s", k, v, err)
		}
	}
	return nil
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
		return 0, fmt.Errorf("type mismatch, expected int in got %T", v)
	}
}

func validateSQLString(s string) error {
	if s == "" {
		return fmt.Errorf("identifier is empty")
	}
	if strings.Contains(s, ";") {
		return fmt.Errorf("identifier contains ';'")
	}
	for _, r := range s {
		// blocks control characters (0x00-0x1F e 0x7F)
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("identifier contains control char")
		}
	}
	return nil
}

// Arrow manager
type arrowMgr struct {
	arrow *duckdb.Arrow
}

func (a *arrowMgr) RegisterRecordBatch(name string, batch arrow.RecordBatch) (release func(), err error) {
	rdr, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
	if err != nil {
		return nil, err
	}
	// duckdb.RegisterView takes ownership of rdr and will release it when the related release function is called
	// to have deterministic control on the release we add one reference counter
	rdr.Retain()

	duckdbRelease, err := a.arrow.RegisterView(rdr, name)
	if err != nil {
		rdr.Release()
		return nil, err
	}
	return func() {
		duckdbRelease()
		rdr.Release()
	}, nil
}

func newArrowManager(conn *sql.Conn) (*arrowMgr, error) {
	var a *duckdb.Arrow
	if err := conn.Raw(func(dc any) error {
		drvConn, ok := dc.(driver.Conn)
		if !ok {
			return fmt.Errorf("ducklake: unexpected driver conn type %T", dc)
		}
		var err error
		a, err = duckdb.NewArrowFromConn(drvConn)
		return err
	}); err != nil {
		return nil, err
	}
	return &arrowMgr{arrow: a}, nil
}

// Export sink
func GetSink() api.Sink {
	return &DuckLakeSink{
		buildArrowDataFn:     buildArrowData,
		buildArrowDataListFn: buildArrowDataList,
	}
}
