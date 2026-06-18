package metastore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/postings"
	utillog "github.com/grafana/loki/v3/pkg/util/log"
	"github.com/grafana/loki/v3/pkg/xcap"
)

// postingsIndexSectionsReader resolves section pointers from postings sections.
// It resolves the stream selector from the label postings, then prunes the
// resolved pointers with the configured bloom predicates.
type postingsIndexSectionsReader struct {
	logger log.Logger
	obj    *dataobj.Object

	// Stream matching configuration
	matchers []*labels.Matcher
	start    time.Time
	end      time.Time

	// Configured bloom-filter predicates (equality only). Resolution drops the
	// ones that name a resolved stream label; this field itself is never mutated.
	predicates []*labels.Matcher

	batchSize int

	initialized bool

	sections     []*postings.Section
	labelReaders []*postings.Reader

	// resolution holds everything the first Read materializes — the matching
	// pointer rows, the predicates applied to them, the paging cursor and read
	// stats. It stays nil until the first Read; resolve() is its sole producer.
	resolution *resolution

	// readSpan for recording observations, it is created once during the first Read.
	readSpan *xcap.Span
}

var (
	_ ArrowRecordBatchReader = (*postingsIndexSectionsReader)(nil)
	_ bloomStatsProvider     = (*postingsIndexSectionsReader)(nil)
)

func newPostingsIndexSectionsReader(
	logger log.Logger,
	obj *dataobj.Object,
	start, end time.Time,
	matchers []*labels.Matcher,
	predicates []*labels.Matcher,
	batchSize int,
) *postingsIndexSectionsReader {
	// Only keep equal predicates for bloom filtering
	var equalPredicates []*labels.Matcher
	for _, p := range predicates {
		if p.Type == labels.MatchEqual {
			equalPredicates = append(equalPredicates, p)
		}
	}

	if batchSize <= 0 {
		batchSize = 8192
	}

	return &postingsIndexSectionsReader{
		logger:     logger,
		obj:        obj,
		matchers:   matchers,
		predicates: equalPredicates,
		batchSize:  batchSize,
		start:      start,
		end:        end,
	}
}

func (r *postingsIndexSectionsReader) Open(ctx context.Context) error {
	if r.initialized {
		return nil
	}

	if err := r.init(ctx); err != nil {
		return err
	}

	r.initialized = true
	return nil
}

func (r *postingsIndexSectionsReader) init(ctx context.Context) error {
	if len(r.matchers) == 0 {
		return nil
	}

	ctx, sp := xcap.StartSpan(ctx, tracer, "metastore.postingsIndexSectionsReader.Open")
	defer sp.End()

	targetTenant, err := user.ExtractOrgID(ctx)
	if err != nil {
		return fmt.Errorf("extracting org ID: %w", err)
	}

	for _, section := range r.obj.Sections() {
		if section.Tenant != targetTenant || !postings.CheckSection(section) {
			continue
		}

		sec, err := postings.Open(ctx, section)
		if err != nil {
			closeAll(r.labelReaders)
			return fmt.Errorf("opening postings section: %w", err)
		}
		r.sections = append(r.sections, sec)

		labelReader, err := r.openLabelReader(ctx, sec)
		if err != nil {
			closeAll(r.labelReaders)
			return err
		}
		r.labelReaders = append(r.labelReaders, labelReader)
		sp.Record(StatMetastorePointerSectionsOpened.Observe(1))
	}

	return nil
}

// openLabelReader builds a generic postings.Reader projecting the label columns,
// with kind == KindLabel pushed down. The projection order is a contract with
// (*streamScan).accumulate, which reads columns 0..6 positionally.
func (r *postingsIndexSectionsReader) openLabelReader(ctx context.Context, sec *postings.Section) (*postings.Reader, error) {
	cols, err := findPostingsColumnsByTypes(
		sec.Columns(),
		postings.ColumnTypeObjectPath,
		postings.ColumnTypeSectionIndex,
		postings.ColumnTypeColumnName,
		postings.ColumnTypeLabelValue,
		postings.ColumnTypeStreamIDBitmap,
		postings.ColumnTypeMinTimestamp,
		postings.ColumnTypeMaxTimestamp,
		postings.ColumnTypeKind,
	)
	if err != nil {
		return nil, fmt.Errorf("finding label columns: %w", err)
	}

	colKind := cols[len(cols)-1]
	reader := postings.NewReader(postings.ReaderOptions{
		Columns: cols,
		Predicates: []postings.Predicate{
			postings.EqualPredicate{Column: colKind, Value: scalar.NewInt64Scalar(int64(postings.KindLabel))},
		},
		Allocator: memory.DefaultAllocator,
	})
	if err := reader.Open(ctx); err != nil {
		return nil, fmt.Errorf("opening label reader: %w", err)
	}
	return reader, nil
}

func (r *postingsIndexSectionsReader) Read(ctx context.Context) (arrow.RecordBatch, error) {
	if !r.initialized {
		return nil, errIndexSectionsReaderNotOpen
	}

	if r.readSpan == nil {
		ctx, r.readSpan = xcap.StartSpan(ctx, tracer, "metastore.postingsIndexSectionsReader.Read")
	} else {
		ctx = xcap.ContextWithSpan(ctx, r.readSpan)
	}

	if r.resolution == nil {
		res, err := r.resolve(ctx)
		if err != nil {
			return nil, err
		}
		r.resolution = res
	}

	if r.resolution.exhausted() {
		return nil, io.EOF
	}

	rec := buildPointersRecord(memory.DefaultAllocator, r.resolution.next(r.batchSize))
	r.readSpan.Record(xcap.StatMetastoreSectionPointersRead.Observe(rec.NumRows()))
	return rec, nil
}

// resolve materializes every matching pointer row: it resolves the stream
// selector from the label postings, then prunes the resolved rows with the bloom
// predicates. It runs once, on the first Read, and never mutates the reader.
func (r *postingsIndexSectionsReader) resolve(ctx context.Context) (*resolution, error) {
	rows, predicates, err := r.resolveStreams(ctx)
	if err != nil {
		return nil, err
	}

	res := &resolution{rows: rows, predicates: predicates}
	if len(res.rows) == 0 {
		// Nothing matched the selector, so there is nothing left to bloom-filter.
		return res, nil
	}
	if err := r.filterPointersByBloom(ctx, res); err != nil {
		return nil, err
	}
	return res, nil
}

// resolveStreams reads label rows from every postings section into a streamScan,
// resolving the stream selector into pointer rows, and collects stream label
// names so stream-label predicates can be dropped from the bloom checks.
func (r *postingsIndexSectionsReader) resolveStreams(ctx context.Context) ([]pointerRow, []*labels.Matcher, error) {
	region := xcap.RegionFromContext(ctx)
	startTime := time.Now()
	defer func() {
		region.Record(xcap.StatMetastoreStreamsReadTime.Observe(time.Since(startTime).Seconds()))
	}()

	acc := newStreamScan(r.matchers, r.start, r.end)

	pointerStart := time.Now()
	for _, reader := range r.labelReaders {
		for {
			rec, err := reader.Read(ctx, r.batchSize)
			if rec != nil && rec.NumRows() > 0 {
				if accErr := acc.accumulate(rec); accErr != nil {
					return nil, nil, fmt.Errorf("accumulating label postings: %w", accErr)
				}
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, nil, fmt.Errorf("reading label postings: %w", err)
			}
		}
	}
	res := acc.finalize(ctx)
	if r.readSpan != nil {
		r.readSpan.Record(xcap.StatMetastoreSectionPointersReadTime.Observe(time.Since(pointerStart).Seconds()))
	}

	region.Record(xcap.StatMetastoreStreamsRead.Observe(int64(len(res.MatchingStreamRefs))))

	streamLabelNames := make(map[string]struct{}, len(res.MatchingLabelColumnNames))
	for _, name := range res.MatchingLabelColumnNames {
		streamLabelNames[name] = struct{}{}
	}
	return res.Pointers, r.bloomPredicates(streamLabelNames), nil
}

// bloomPredicates returns the configured predicates that do not name a resolved
// stream label. Blooms only index structured-metadata columns, so bloom-testing
// a stream label would falsely prune sections.
func (r *postingsIndexSectionsReader) bloomPredicates(streamLabelNames map[string]struct{}) []*labels.Matcher {
	filtered := make([]*labels.Matcher, 0, len(r.predicates))
	for _, predicate := range r.predicates {
		if _, isStreamLabel := streamLabelNames[predicate.Name]; !isStreamLabel {
			filtered = append(filtered, predicate)
		}
	}
	return filtered
}

// filterPointersByBloom drops resolved pointer rows whose (object, section) did
// not match every bloom predicate. It is a no-op when there are no predicates.
func (r *postingsIndexSectionsReader) filterPointersByBloom(ctx context.Context, res *resolution) error {
	if len(res.predicates) == 0 {
		return nil
	}

	matchedSectionKeys, err := r.readMatchedSectionKeys(ctx, res.predicates)
	if err != nil {
		return fmt.Errorf("reading matched section keys: %w", err)
	}
	res.rows = filterRowsBySectionKeys(res.rows, matchedSectionKeys)
	if len(res.rows) == 0 {
		level.Debug(utillog.WithContext(ctx, r.logger)).Log("msg", "no sections resolved", "reason", "no matching predicates")
	}
	return nil
}

// resolution is the materialized output of resolve: the matching pointer rows,
// the bloom predicates applied to them (stream labels already dropped), a cursor
// paging the rows out, and a running count of rows emitted.
type resolution struct {
	rows       []pointerRow
	predicates []*labels.Matcher
	offset     int
	rowsRead   uint64
}

func (res *resolution) exhausted() bool {
	return res.offset >= len(res.rows)
}

// next returns the next slice of up to batchSize rows, advancing the cursor and
// the emitted-row counter.
func (res *resolution) next(batchSize int) []pointerRow {
	start := res.offset
	end := min(start+batchSize, len(res.rows))
	res.offset = end
	res.rowsRead += uint64(end - start)
	return res.rows[start:end]
}

// filterRowsBySectionKeys returns the pointer rows whose (object path, section)
// tuple is present in keys, preserving order.
func filterRowsBySectionKeys(rows []pointerRow, keys map[SectionKey]struct{}) []pointerRow {
	n := 0
	for _, row := range rows {
		sk := SectionKey{ObjectPath: row.ObjectPath, SectionIdx: row.SectionIndex}
		if _, ok := keys[sk]; ok {
			rows[n] = row
			n++
		}
	}
	return rows[:n]
}

// readMatchedSectionKeys reads bloom rows from every postings section and
// returns the section keys matching every bloom predicate.
func (r *postingsIndexSectionsReader) readMatchedSectionKeys(ctx context.Context, predicates []*labels.Matcher) (map[SectionKey]struct{}, error) {
	predicateNames := make([]string, 0, len(predicates))
	for _, predicate := range predicates {
		predicateNames = append(predicateNames, predicate.Name)
	}

	var bloomReaders []*postings.Reader
	defer func() { closeAll(bloomReaders) }()

	var recs []arrow.RecordBatch
	for _, sec := range r.sections {
		reader, err := r.openBloomReader(ctx, sec, predicateNames)
		if err != nil {
			return nil, err
		}
		bloomReaders = append(bloomReaders, reader)

		for {
			rec, err := reader.Read(ctx, r.batchSize)
			if rec != nil && rec.NumRows() > 0 {
				recs = append(recs, rec)
			}
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("reading bloom postings: %w", err)
			}
		}
	}

	if len(recs) == 0 {
		return map[SectionKey]struct{}{}, nil
	}

	return matchSections(ctx, recs, predicates)
}

// openBloomReader builds a generic postings.Reader projecting the bloom columns,
// with kind == KindBloom (and an optional column-name filter) pushed down. The
// projection order is a contract with matchSections, which reads columns 0..3.
func (r *postingsIndexSectionsReader) openBloomReader(ctx context.Context, sec *postings.Section, predicateNames []string) (*postings.Reader, error) {
	cols, err := findPostingsColumnsByTypes(
		sec.Columns(),
		postings.ColumnTypeObjectPath,
		postings.ColumnTypeSectionIndex,
		postings.ColumnTypeColumnName,
		postings.ColumnTypeBloomFilter,
		postings.ColumnTypeKind,
	)
	if err != nil {
		return nil, fmt.Errorf("finding bloom columns: %w", err)
	}

	colColumnName := cols[2]
	colKind := cols[len(cols)-1]
	preds := []postings.Predicate{
		postings.EqualPredicate{Column: colKind, Value: scalar.NewInt64Scalar(int64(postings.KindBloom))},
	}
	if namePred := bloomColumnNamePredicate(colColumnName, predicateNames); namePred != nil {
		preds = append(preds, namePred)
	}

	reader := postings.NewReader(postings.ReaderOptions{
		Columns:    cols,
		Predicates: preds,
		Allocator:  memory.DefaultAllocator,
	})
	if err := reader.Open(ctx); err != nil {
		return nil, fmt.Errorf("opening bloom reader: %w", err)
	}
	return reader, nil
}

// bloomColumnNamePredicate builds an optional column-name pushdown to avoid
// scanning bloom rows for columns no predicate references. Returns nil when
// there are no usable names.
func bloomColumnNamePredicate(colColumnName *postings.Column, columnNames []string) postings.Predicate {
	uniq := make(map[string]struct{}, len(columnNames))
	values := make([]scalar.Scalar, 0, len(columnNames))
	for _, name := range columnNames {
		if name == "" {
			continue
		}
		if _, exists := uniq[name]; exists {
			continue
		}
		uniq[name] = struct{}{}
		values = append(values, scalar.NewStringScalar(name))
	}

	switch len(values) {
	case 0:
		return nil
	case 1:
		return postings.EqualPredicate{Column: colColumnName, Value: values[0]}
	default:
		return postings.InPredicate{Column: colColumnName, Values: values}
	}
}

// findPostingsColumnsByTypes returns the columns whose Type matches each
// requested type, in the requested order. Errors if any type is absent.
func findPostingsColumnsByTypes(cols []*postings.Column, types ...postings.ColumnType) ([]*postings.Column, error) {
	result := make([]*postings.Column, 0, len(types))
	for _, t := range types {
		var found *postings.Column
		for _, c := range cols {
			if c.Type == t {
				found = c
				break
			}
		}
		if found == nil {
			return nil, fmt.Errorf("finding postings column %s: not found", t)
		}
		result = append(result, found)
	}
	return result, nil
}

// Close releases the label readers held by this postingsIndexSectionsReader.
func (r *postingsIndexSectionsReader) Close() {
	closeAll(r.labelReaders)

	if r.readSpan != nil {
		r.readSpan.End()
	}
}

func (r *postingsIndexSectionsReader) totalReadRows() uint64 {
	if r.resolution == nil {
		return 0
	}
	return r.resolution.rowsRead
}

const (
	streamLabelNamesField        = "__streamLabelNames__"
	pointerKindStreamIndex int64 = 1
)

var postingsPointersSchema = newPointersRecordSchema()

func pointersRecordSchema() *arrow.Schema {
	return postingsPointersSchema
}

func newPointersRecordSchema() *arrow.Schema {
	mkField := func(label, typeName string, dty arrow.DataType) arrow.Field {
		name := typeName + "." + dty.Name()
		if label != "" {
			name = label + "." + name
		}
		return arrow.Field{Name: name, Type: dty, Nullable: true}
	}
	fields := []arrow.Field{
		// The path column carries Tag="path"; all others have an empty Tag.
		mkField("path", "path", arrow.BinaryTypes.String),
		mkField("", "section", arrow.PrimitiveTypes.Int64),
		mkField("", "pointer_kind", arrow.PrimitiveTypes.Int64),
		mkField("", "stream_id", arrow.PrimitiveTypes.Int64),
		mkField("", "stream_id_ref", arrow.PrimitiveTypes.Int64),
		mkField("", "min_timestamp", arrow.FixedWidthTypes.Timestamp_ns),
		mkField("", "max_timestamp", arrow.FixedWidthTypes.Timestamp_ns),
		mkField("", "row_count", arrow.PrimitiveTypes.Int64),
		mkField("", "uncompressed_size", arrow.PrimitiveTypes.Int64),
		// Always null on the postings path; present only so the schema matches the
		// streams+pointers reader that CollectSections also consumes.
		{Name: streamLabelNamesField, Type: arrow.BinaryTypes.String, Nullable: true},
	}
	return arrow.NewSchema(fields, nil)
}

// buildPointersRecord builds a batch in [pointersRecordSchema] field order from
// the pointer rows resolved out of the postings section.
func buildPointersRecord(alloc memory.Allocator, rows []pointerRow) arrow.RecordBatch {
	rb := array.NewRecordBuilder(alloc, pointersRecordSchema())

	for _, row := range rows {
		rb.Field(0).(*array.StringBuilder).Append(row.ObjectPath)
		rb.Field(1).(*array.Int64Builder).Append(row.SectionIndex)
		rb.Field(2).(*array.Int64Builder).Append(pointerKindStreamIndex)
		rb.Field(3).(*array.Int64Builder).Append(row.StreamID)
		rb.Field(4).(*array.Int64Builder).Append(row.StreamID)
		rb.Field(5).(*array.TimestampBuilder).Append(arrow.Timestamp(row.MinTimestamp))
		rb.Field(6).(*array.TimestampBuilder).Append(arrow.Timestamp(row.MaxTimestamp))
		rb.Field(7).(*array.Int64Builder).Append(0)
		rb.Field(8).(*array.Int64Builder).Append(0)
		rb.Field(9).(*array.StringBuilder).AppendNull()
	}

	return rb.NewRecordBatch()
}
