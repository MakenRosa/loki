package metastore

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/loki/v3/pkg/dataobj/sections/postings"
	"github.com/grafana/loki/v3/pkg/xcap"
)

// streamScan resolves a stream selector against the label postings of one or
// more postings sections. Feed each section's KindLabel record batches with
// [streamScan.accumulate], then call [streamScan.finalize] once.

// streamRef identifies a stream in postings rows by object path and stream ID.
// Stream IDs are only unique within a source logs object, so joining across
// postings rows from multiple objects must keep object context.
type streamRef struct {
	ObjectPath string
	StreamID   int64
}

// pointerRow is one deduplicated (object, section, stream) tuple produced by the
// label scan, carrying the time bounds merged across that stream's label
// postings.
type pointerRow struct {
	ObjectPath   string
	SectionIndex int64
	StreamID     int64
	MinTimestamp int64
	MaxTimestamp int64
	HasBounds    bool
}

type pointerRowKey struct {
	objectPath   string
	sectionIndex int64
	streamID     int64
}

func mergePointerRowBounds(row *pointerRow, minTs, maxTs int64, hasBounds bool) {
	if !hasBounds {
		return
	}
	if !row.HasBounds {
		row.MinTimestamp = minTs
		row.MaxTimestamp = maxTs
		row.HasBounds = true
		return
	}
	if minTs < row.MinTimestamp {
		row.MinTimestamp = minTs
	}
	if maxTs > row.MaxTimestamp {
		row.MaxTimestamp = maxTs
	}
}

type matcherIndex int

type streamScanResult struct {
	MatchingStreamRefs map[streamRef]struct{}
	LabelColumnNames   []string
	// MatchingLabelColumnNames holds names observed on the matched streams only,
	// unlike LabelColumnNames which spans every label row.
	MatchingLabelColumnNames []string
	Pointers                 []pointerRow
}

// streamScan holds the cross-section accumulators for one logical pass over KindLabel rows.
type streamScan struct {
	activeMatchers     []*labels.Matcher
	perMatcherStreams  map[matcherIndex]map[streamRef]struct{}
	perMatcherHasName  map[matcherIndex]map[streamRef]struct{}
	matchersByName     map[string][]matcherIndex
	missingMatches     []bool
	allStreams         map[streamRef]struct{}
	allColumnNames     map[string]struct{}
	labelNamesByStream map[streamRef]map[string]struct{}
	pointerRows        []pointerRow
	seen               map[pointerRowKey]int
	startNanos         int64
	endNanos           int64
}

// newStreamScan returns an accumulator for resolving a stream selector across
// one or more postings sections.
func newStreamScan(matchers []*labels.Matcher, start, end time.Time) *streamScan {
	active := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if m == nil {
			continue
		}
		active = append(active, m)
	}

	matchersByName := make(map[string][]matcherIndex, len(active))
	perMatcherStreams := make(map[matcherIndex]map[streamRef]struct{}, len(active))
	perMatcherHasName := make(map[matcherIndex]map[streamRef]struct{}, len(active))
	missingMatches := make([]bool, len(active))
	for i, m := range active {
		idx := matcherIndex(i)
		matchersByName[m.Name] = append(matchersByName[m.Name], idx)
		perMatcherStreams[idx] = make(map[streamRef]struct{})
		perMatcherHasName[idx] = make(map[streamRef]struct{})
		missingMatches[i] = matcherMatchesMissingLabel(m)
	}

	return &streamScan{
		activeMatchers:     active,
		perMatcherStreams:  perMatcherStreams,
		perMatcherHasName:  perMatcherHasName,
		matchersByName:     matchersByName,
		missingMatches:     missingMatches,
		allStreams:         make(map[streamRef]struct{}),
		allColumnNames:     make(map[string]struct{}),
		labelNamesByStream: make(map[streamRef]map[string]struct{}),
		seen:               make(map[pointerRowKey]int),
		startNanos:         start.UnixNano(),
		endNanos:           end.UnixNano(),
	}
}

func matcherMatchesMissingLabel(m *labels.Matcher) bool {
	switch m.Type {
	case labels.MatchEqual:
		return m.Value == ""
	case labels.MatchNotEqual:
		return m.Value != ""
	case labels.MatchRegexp, labels.MatchNotRegexp:
		return m.Matches("")
	default:
		return false
	}
}

// accumulate processes one batch of the unified KindLabel scan, feeding three
// accumulators from a single decode of each row's bitmap:
//   - perMatcherStreams: streams satisfying each matcher (matcher eval; ignores time)
//   - allColumnNames: every distinct label name (independent of matchers and time)
//   - pointerRows/seen: per-(object,section,stream) time bounds for ALL streams,
//     pruned to rows overlapping [startNanos,endNanos]; scoped to matching streams
//     by finalize.
//
// Column order is a contract with the projection built by the metastore label
// reader: object_path, section_index, column_name, label_value,
// stream_id_bitmap, min_timestamp, max_timestamp (kind, if projected, is ignored).
func (acc *streamScan) accumulate(rb arrow.RecordBatch) error {
	if rb.NumRows() == 0 {
		return nil
	}
	objectPathCol, ok := rb.Column(0).(*array.String)
	if !ok {
		return fmt.Errorf("postings label scan: object_path has unexpected type %T", rb.Column(0))
	}
	sectionIndexCol, ok := rb.Column(1).(*array.Int64)
	if !ok {
		return fmt.Errorf("postings label scan: section_index has unexpected type %T", rb.Column(1))
	}
	columnNameCol, ok := rb.Column(2).(*array.String)
	if !ok {
		return fmt.Errorf("postings label scan: column_name has unexpected type %T", rb.Column(2))
	}
	labelValueCol, ok := rb.Column(3).(*array.String)
	if !ok {
		return fmt.Errorf("postings label scan: label_value has unexpected type %T", rb.Column(3))
	}
	bitmapCol, ok := rb.Column(4).(*array.Binary)
	if !ok {
		return fmt.Errorf("postings label scan: stream_id_bitmap has unexpected type %T", rb.Column(4))
	}
	minTsCol, ok := rb.Column(5).(*array.Timestamp)
	if !ok {
		return fmt.Errorf("postings label scan: min_timestamp has unexpected type %T", rb.Column(5))
	}
	maxTsCol, ok := rb.Column(6).(*array.Timestamp)
	if !ok {
		return fmt.Errorf("postings label scan: max_timestamp has unexpected type %T", rb.Column(6))
	}

	for i := 0; i < int(rb.NumRows()); i++ {
		// Full label-name set: any non-empty column_name, independent of the
		// matcher and time logic below.
		if !columnNameCol.IsNull(i) {
			if name := columnNameCol.Value(i); name != "" {
				acc.allColumnNames[name] = struct{}{}
			}
		}

		// Both the matcher and pointer accumulators need object_path + bitmap.
		if objectPathCol.IsNull(i) || bitmapCol.IsNull(i) {
			continue
		}
		objectPath := objectPathCol.Value(i)
		bitmapBytes := bitmapCol.Value(i)

		// Matchers this row satisfies (needs column_name + label_value).
		var targeted []matcherIndex
		matchedSmall := [8]matcherIndex{}
		matched := matchedSmall[:0]
		labelName := ""
		hasLabelName := false
		if !columnNameCol.IsNull(i) && !labelValueCol.IsNull(i) {
			name := columnNameCol.Value(i)
			value := labelValueCol.Value(i)
			labelName = name
			hasLabelName = name != ""
			targeted = acc.matchersByName[name]
			if len(targeted) > cap(matched) {
				matched = make([]matcherIndex, 0, len(targeted))
			}
			for _, idx := range targeted {
				if acc.activeMatchers[idx].Matches(value) {
					matched = append(matched, idx)
				}
			}
		}

		// Whether this row contributes pointer bounds (needs section_index;
		// time-filtered). A null bound can't be pruned, so keep it and emit zero.
		boundsOK := false
		var sectionIndex int64
		var minTs, maxTs int64
		hasBounds := false
		if !sectionIndexCol.IsNull(i) {
			sectionIndex = sectionIndexCol.Value(i)
			if !minTsCol.IsNull(i) && !maxTsCol.IsNull(i) {
				hasBounds = true
				minTs = int64(minTsCol.Value(i))
				maxTs = int64(maxTsCol.Value(i))
				boundsOK = !(maxTs < acc.startNanos || minTs > acc.endNanos)
			} else {
				boundsOK = true
			}
		}

		if len(matched) == 0 && !boundsOK {
			continue
		}

		postings.RangeStreamIDs(bitmapBytes, func(streamID int64) {
			ref := streamRef{ObjectPath: objectPath, StreamID: streamID}
			acc.allStreams[ref] = struct{}{}
			if hasLabelName {
				names := acc.labelNamesByStream[ref]
				if names == nil {
					names = make(map[string]struct{})
					acc.labelNamesByStream[ref] = names
				}
				names[labelName] = struct{}{}
			}

			for _, idx := range targeted {
				acc.perMatcherHasName[idx][ref] = struct{}{}
			}

			for _, idx := range matched {
				acc.perMatcherStreams[idx][ref] = struct{}{}
			}

			if boundsOK {
				key := pointerRowKey{objectPath: objectPath, sectionIndex: sectionIndex, streamID: streamID}
				if idx, exists := acc.seen[key]; exists {
					mergePointerRowBounds(&acc.pointerRows[idx], minTs, maxTs, hasBounds)
				} else {
					row := pointerRow{ObjectPath: objectPath, SectionIndex: sectionIndex, StreamID: streamID}
					mergePointerRowBounds(&row, minTs, maxTs, hasBounds)
					acc.pointerRows = append(acc.pointerRows, row)
					acc.seen[key] = len(acc.pointerRows) - 1
				}
			}
		})
	}
	return nil
}

// finalize ANDs the per-matcher stream sets and scopes the accumulated pointer
// rows to the surviving streams. It must be called once, after every section has
// been accumulated into the streamScan.
func (acc *streamScan) finalize(ctx context.Context) *streamScanResult {
	acc.applyMissingLabelSemantics()
	matchingStreams := intersectMatcherStreams(acc.perMatcherStreams, len(acc.activeMatchers))

	labelNames := make([]string, 0, len(acc.allColumnNames))
	for name := range acc.allColumnNames {
		labelNames = append(labelNames, name)
	}

	matchingLabelNamesMap := make(map[string]struct{})
	for ref := range matchingStreams {
		names := acc.labelNamesByStream[ref]
		for name := range names {
			matchingLabelNamesMap[name] = struct{}{}
		}
	}
	matchingLabelNames := make([]string, 0, len(matchingLabelNamesMap))
	for name := range matchingLabelNamesMap {
		matchingLabelNames = append(matchingLabelNames, name)
	}

	// Scope the accumulated pointer rows (all streams) to the matching streams,
	// preserving first-seen order.
	var matchedRows []pointerRow
	for _, row := range acc.pointerRows {
		ref := streamRef{ObjectPath: row.ObjectPath, StreamID: row.StreamID}
		if _, ok := matchingStreams[ref]; ok {
			matchedRows = append(matchedRows, row)
		}
	}

	xcap.RegionFromContext(ctx).Record(xcap.StatPostingsLabelsResolved.Observe(int64(len(matchingStreams))))
	xcap.RegionFromContext(ctx).Record(xcap.StatPostingsPointersRead.Observe(int64(len(matchedRows))))
	return &streamScanResult{
		MatchingStreamRefs:       matchingStreams,
		LabelColumnNames:         labelNames,
		MatchingLabelColumnNames: matchingLabelNames,
		Pointers:                 matchedRows,
	}
}

func (acc *streamScan) applyMissingLabelSemantics() {
	if len(acc.activeMatchers) == 0 || len(acc.allStreams) == 0 {
		return
	}

	for i := range acc.activeMatchers {
		if !acc.missingMatches[i] {
			continue
		}
		idx := matcherIndex(i)
		matched := acc.perMatcherStreams[idx]
		hasName := acc.perMatcherHasName[idx]
		for ref := range acc.allStreams {
			if _, has := hasName[ref]; has {
				continue
			}
			matched[ref] = struct{}{}
		}
	}
}

// intersectMatcherStreams ANDs the per-matcher stream sets: a stream survives
// only if it appears under every matcher.
func intersectMatcherStreams(perMatcherStreams map[matcherIndex]map[streamRef]struct{}, activeMatchers int) map[streamRef]struct{} {
	if activeMatchers == 0 {
		return make(map[streamRef]struct{})
	}
	first := perMatcherStreams[matcherIndex(0)]
	matchingStreams := make(map[streamRef]struct{}, len(first))
	for streamRef := range first {
		matchingStreams[streamRef] = struct{}{}
	}
	for i := 1; i < activeMatchers && len(matchingStreams) > 0; i++ {
		next := perMatcherStreams[matcherIndex(i)]
		for streamRef := range matchingStreams {
			if _, ok := next[streamRef]; !ok {
				delete(matchingStreams, streamRef)
			}
		}
	}
	return matchingStreams
}
