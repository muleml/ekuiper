package ducklake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
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
	viewSeq        uint64
	ducklakeName   string
	Fields         []string `json:"fields"`

	Storage StorageConf
	Catalog CatalogConf
}

type DuckLakeSink struct {
	conf                 c
	db                   sqlPool
	conn                 sqlEngine
	arrowMgr             arrowManager
	buildArrowDataFn     func(api.StreamContext, map[string]any, []string) (arrow.RecordBatch, error)
	buildArrowDataListFn func(api.StreamContext, []map[string]any, []string) (arrow.RecordBatch, error)
}

var _ model.SinkInfoNode = &DuckLakeSink{}

func (d *DuckLakeSink) Provision(ctx api.StreamContext, props map[string]any) error {
	if err := d.configure(props); err != nil {
		return err
	}

	ctx.GetLogger().Infof("ducklake sink provision successfully terminated")

	return nil
}

func (d *DuckLakeSink) configure(props map[string]any) error {
	d.conf = c{
		Catalog: CatalogConf{
			Type: "duckdb",
		},
		ducklakeName: "the_ducklake",
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
	if err := validateFields(d.conf.Fields); err != nil {
		return fmt.Errorf("error configuring ducklake sink: %s", err)
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
	if err := d.configure(props); err != nil {
		return err
	}

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
	arrowData, err := d.buildArrowDataFn(ctx, data, d.conf.Fields)
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
	arrowData, err := d.buildArrowDataListFn(ctx, data, d.conf.Fields)
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
	viewName := fmt.Sprintf("__ekuiper_ducklake_%d", atomic.AddUint64(&d.conf.viewSeq, 1))

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
	fields, err := fieldsFromSchema(schema)
	if err != nil {
		return "", err
	}
	columns := ""
	for i, field := range fields {
		if i > 0 {
			columns += ", "
		}
		columns += field
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM %s;", d.conf.ducklakeName, d.conf.sanitizedTable, columns, columns, viewName)
	return query, nil
}

// fieldsFromSchema returns the column names of the arrow batch schema, in order.
// It is the only source of truth for the INSERT/SELECT column list: the schema is
// built from the declared fields or from the batch union, so list and view cannot diverge.
// Every name is validated as a plain identifier because it is interpolated in the INSERT
// statement without quoting.
func fieldsFromSchema(schema *arrow.Schema) ([]string, error) {
	if len(schema.Fields()) == 0 {
		return nil, fmt.Errorf("empty arrow schema")
	}
	fields := make([]string, 0, len(schema.Fields()))
	for _, field := range schema.Fields() {
		if err := validateIdentifier(field.Name); err != nil {
			return nil, err
		}
		fields = append(fields, field.Name)
	}
	return fields, nil
}

// isPlainIdentifier reports whether s is a plain SQL identifier: letters, digits and
// underscore, not starting with a digit. Column names are interpolated in the INSERT
// statement, so anything else is rejected instead of quoted.
func isPlainIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_':
			// always allowed
		case r >= '0' && r <= '9' && i > 0:
			// digits are allowed only after the first character
		default:
			return false
		}
	}
	return true
}

// validateIdentifier rejects names that cannot be interpolated safely in a query.
func validateIdentifier(name string) error {
	if !isPlainIdentifier(name) {
		return fmt.Errorf("invalid field name %q: use only letters, digits and underscore, and do not start with a digit", name)
	}
	return nil
}

// validateFields rejects declared fields that cannot identify a column unambiguously.
func validateFields(fields []string) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if strings.TrimSpace(field) == "" {
			return fmt.Errorf("empty field in fields")
		}
		if _, ok := seen[field]; ok {
			return fmt.Errorf("duplicated field %q", field)
		}
		seen[field] = struct{}{}
		if err := validateIdentifier(field); err != nil {
			return err
		}
	}
	return nil
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
	return model.SinkInfo{HasFields: len(d.conf.Fields) > 0}
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

	query = fmt.Sprintf("ATTACH 'ducklake:ducklake_secret' AS %s;", d.conf.ducklakeName)
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
		dataPath = fmt.Sprintf("s3://%s", conf.Bucket)
	default:
		return "", fmt.Errorf("error connecting ducklake sink: storage type not supported")
	}

	secret := fmt.Sprintf("CREATE OR REPLACE SECRET ducklake_secret (TYPE ducklake, METADATA_PATH '%s', DATA_PATH '%s', METADATA_PARAMETERS MAP {%s});", metaDataPath, dataPath, metaDataParametersMap)
	return secret, nil
}

// buildArrowData builds the arrow record batch for a single row.
func buildArrowData(ctx api.StreamContext, row map[string]any, fields []string) (arrow.RecordBatch, error) {
	return buildArrowDataList(ctx, []map[string]any{row}, fields)
}

// buildArrowDataList builds the arrow record batch for a batch of rows.
// Map-empty rows are skipped because they would only produce all-NULL rows; the
// row number reported in the error wrapper always refers to the index in the
// original slice, so the meaning of the "Row <n>" messages is preserved.
// Missing values are reported once per batch instead of once per value.
// The schema is driven by the declared fields when they are provided (declared
// order), otherwise it is inferred from the union of the non-nil keys of the
// usable rows.
func buildArrowDataList(ctx api.StreamContext, data []map[string]any, fields []string) (arrow.RecordBatch, error) {
	if len(data) < 1 {
		ctx.GetLogger().Errorf("Ducklake sink: empty data")
		return nil, nil
	}

	rows := make([]map[string]any, 0, len(data))
	rowNums := make([]int, 0, len(data))
	for rowNum, row := range data {
		if len(row) == 0 {
			continue
		}
		rows = append(rows, row)
		rowNums = append(rowNums, rowNum)
	}
	if skipped := len(data) - len(rows); skipped > 0 {
		ctx.GetLogger().Debugf("ducklake sink: skipped %d empty row(s)", skipped)
	}
	if len(rows) == 0 {
		return nil, nil
	}

	var (
		arrowInferredSchema *inferredSchema
		err                 error
	)
	if len(fields) > 0 {
		arrowInferredSchema, err = arrowSchemaFromFields(fields, rows)
	} else {
		arrowInferredSchema, err = inferArrowSchemaFromRows(rows)
	}
	if err != nil {
		return nil, fmt.Errorf("Ducklake sink error creating arrow data: %s", err)
	}
	if arrowInferredSchema == nil {
		return nil, nil
	}

	if len(fields) > 0 {
		warnUndeclaredFields(ctx, arrowInferredSchema, rows)
	}

	rb := array.NewRecordBuilder(memory.DefaultAllocator, arrowInferredSchema.schema)
	defer rb.Release()

	missingFields := make([]string, 0)
	for i, row := range rows {
		missing, err := appendRow(rb, arrowInferredSchema, row)
		if err != nil {
			return nil, fmt.Errorf("Ducklake sink error creating arrow data: %s - Row <%d>", err, rowNums[i])
		}
		missingFields = append(missingFields, missing...)
	}
	logMissingValues(ctx, missingFields)
	return rb.NewRecordBatch(), nil
}

// warnUndeclaredFields logs one warning per key that appears in the rows but is
// not part of the declared schema. Undeclared keys are already ignored because
// appendRow iterates over schema.keys only; this helper just surfaces them.
func warnUndeclaredFields(ctx api.StreamContext, schema *inferredSchema, rows []map[string]any) {
	declared := make(map[string]struct{}, len(schema.keys))
	for _, k := range schema.keys {
		declared[k] = struct{}{}
	}
	seen := make(map[string]struct{})
	undeclared := make([]string, 0)
	for _, row := range rows {
		for k := range row {
			if _, ok := declared[k]; ok {
				continue
			}
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			undeclared = append(undeclared, k)
		}
	}
	sort.Strings(undeclared)
	for _, k := range undeclared {
		ctx.GetLogger().Warnf("ducklake sink: ignoring undeclared field <%s>", k)
	}
}

// arrowTypeForValue derives the arrow data type from the Go type of v.
// ok is false when the value type is not supported.
func arrowTypeForValue(v any) (arrow.DataType, bool) {
	switch v.(type) {
	case string:
		return arrow.BinaryTypes.String, true
	case bool:
		return arrow.FixedWidthTypes.Boolean, true
	case float32, float64:
		return arrow.PrimitiveTypes.Float64, true
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return arrow.PrimitiveTypes.Int64, true
	case time.Time:
		return arrow.FixedWidthTypes.Timestamp_ms, true
	default:
		return nil, false
	}
}

// arrowSchemaFromFields builds the batch schema from the declared fields, keeping their order.
// The column type is derived from the first non-nil value found while scanning rows; a declared
// column that no row values is mapped to arrow.BinaryTypes.String.
func arrowSchemaFromFields(fields []string, rows []map[string]any) (*inferredSchema, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	keys := make([]string, len(fields))
	copy(keys, fields)

	arrowFields := make([]arrow.Field, len(keys))
	dts := make([]arrow.DataType, len(keys))
	for i, name := range keys {
		var dt arrow.DataType = arrow.BinaryTypes.String
		for _, row := range rows {
			v, ok := row[name]
			if !ok || v == nil {
				continue
			}
			t, supported := arrowTypeForValue(v)
			if !supported {
				return nil, fmt.Errorf("field <%s> has unsupported type <%T>", name, v)
			}
			dt = t
			break
		}
		arrowFields[i] = arrow.Field{Name: name, Type: dt, Nullable: true}
		dts[i] = dt
	}
	return &inferredSchema{
		keys:   keys,
		schema: arrow.NewSchema(arrowFields, nil),
		dts:    dts,
	}, nil
}

// inferArrowSchemaFromRows builds the schema from the union of the non-nil keys of all rows,
// sorted alphabetically; the type comes from the first value met for each key.
// It returns (nil, nil) when no row carries a usable value.
func inferArrowSchemaFromRows(rows []map[string]any) (*inferredSchema, error) {
	dtsByKey := make(map[string]arrow.DataType)
	for _, row := range rows {
		for k, v := range row {
			if v == nil {
				continue
			}
			if _, ok := dtsByKey[k]; ok {
				continue
			}
			dt, supported := arrowTypeForValue(v)
			if !supported {
				return nil, fmt.Errorf("field <%s> has unsupported type <%T>", k, v)
			}
			dtsByKey[k] = dt
		}
	}
	if len(dtsByKey) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(dtsByKey))
	for k := range dtsByKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fields := make([]arrow.Field, len(keys))
	dts := make([]arrow.DataType, len(keys))
	for i, k := range keys {
		dt := dtsByKey[k]
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
			return fmt.Errorf("type mismatch, expected string, got <%T>", v)
		}
		b.(*array.StringBuilder).Append(s)
	case arrow.BOOL:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("type mismatch, expected bool, got <%T>", v)
		}
		b.(*array.BooleanBuilder).Append(x)
	case arrow.FLOAT64:
		switch x := v.(type) {
		case float32:
			b.(*array.Float64Builder).Append(float64(x))
		case float64:
			b.(*array.Float64Builder).Append(x)
		default:
			return fmt.Errorf("type mismatch, expected float, got <%T>", v)
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
			return fmt.Errorf("type mismatch, expected timestamp, got <%T>", v)
		}
		b.(*array.TimestampBuilder).Append(arrow.Timestamp(t.UnixMilli()))
	default:
		return fmt.Errorf("unexpected arrow type <%s>", dt)
	}
	return nil
}

// appendRow appends one row to the builder and returns the keys without a usable value,
// so the caller can report them once per batch.
func appendRow(rb *array.RecordBuilder, schema *inferredSchema, row map[string]any) ([]string, error) {
	var missing []string
	for colIdx, k := range schema.keys {
		v, ok := row[k]
		if !ok || v == nil {
			missing = append(missing, k)
			rb.Field(colIdx).AppendNull()
			continue
		}
		if err := appendValueToField(rb.Field(colIdx), schema.dts[colIdx], v); err != nil {
			return nil, fmt.Errorf("Field <%s> Value <%s> - %s", k, v, err)
		}
	}
	return missing, nil
}

// logMissingValues reports once per batch which columns had no value, at debug level:
// missing values are expected for sparse measures, not errors.
func logMissingValues(ctx api.StreamContext, missing []string) {
	if len(missing) == 0 {
		return
	}
	seen := make(map[string]struct{}, len(missing))
	uniq := make([]string, 0, len(missing))
	for _, k := range missing {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		uniq = append(uniq, k)
	}
	sort.Strings(uniq)
	ctx.GetLogger().Debugf("ducklake sink: %d empty value(s) filled with NULL for field(s) %s", len(missing), strings.Join(uniq, ", "))
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
		if uint64(x) > math.MaxInt64 {
			return 0, fmt.Errorf("value <%d> overflows int64", x)
		}
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("value <%d> overflows int64", x)
		}
		return int64(x), nil
	default:
		return 0, fmt.Errorf("type mismatch, expected int, got <%T>", v)
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
