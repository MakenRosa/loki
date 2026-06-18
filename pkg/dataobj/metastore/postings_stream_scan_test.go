package metastore

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/postings"
)

func TestStreamScan_EqualMatchers_Single(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2, 3}},
		{name: "env", value: "staging", streamIDs: []int64{4, 5}},
		{name: "app", value: "foo", streamIDs: []int64{2, 3, 6}},
	})

	got, names := resolveToStreamIDs(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
	})
	require.Len(t, got, 3, "env=prod must yield exactly 3 streams")
	for _, id := range []int64{1, 2, 3} {
		_, ok := got[id]
		require.True(t, ok, "stream %d must be present in env=prod result", id)
	}
	require.ElementsMatch(t, []string{"env", "app"}, names,
		"the resolution scan must return every distinct label name in the section")
}

func TestStreamScan_RegexFallback(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2, 3}},
		{name: "env", value: "staging", streamIDs: []int64{4, 5}},
		{name: "app", value: "foo", streamIDs: []int64{2, 3, 6}},
	})

	got, _ := resolveToStreamIDs(t, obj, []*labels.Matcher{
		regexMatcher(t, "env", "^pr.*"),
	})
	require.Len(t, got, 3, "regex env=~^pr.* must match exactly 3 streams (rows with env=prod)")
	for _, id := range []int64{1, 2, 3} {
		_, ok := got[id]
		require.True(t, ok, "stream %d must be present in regex result", id)
	}
}

func TestStreamScan_ReturnsAllLabelNames(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2}},
		{name: "app", value: "foo", streamIDs: []int64{2, 7}},
		{name: "region", value: "us", streamIDs: []int64{2, 8}},
	})

	got, names := resolveToStreamIDs(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
		equalMatcher(t, "app", "foo"),
		equalMatcher(t, "region", "us"),
	})
	require.Len(t, got, 1, "only stream 2 appears under all 3 labels")
	_, ok := got[2]
	require.True(t, ok, "stream 2 must be the sole survivor of the 3-way AND")
	require.ElementsMatch(t, []string{"env", "app", "region"}, names,
		"names must be the full distinct label-name set across all label rows")
}

func TestStreamScan_Mixed_Equal_And_Regex_DifferentNames(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2, 3}},
		{name: "app", value: "foo", streamIDs: []int64{2, 4}},
		{name: "app", value: "bar", streamIDs: []int64{2, 5}},
	})

	got, names := resolveToStreamIDs(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
		regexMatcher(t, "app", "^foo.*"),
	})
	require.Len(t, got, 1,
		"env=prod AND app=~^foo.* must yield exactly stream 2")
	_, ok := got[2]
	require.True(t, ok, "stream 2 (env=prod AND app=foo) must be the sole survivor")
	_, has1 := got[1]
	require.False(t, has1, "stream 1 (env=prod only, no app=foo*) must NOT appear")
	_, has3 := got[3]
	require.False(t, has3, "stream 3 (env=prod only, no app=foo*) must NOT appear")
	_, has4 := got[4]
	require.False(t, has4, "stream 4 (app=foo but no env=prod) must NOT appear")

	require.ElementsMatch(t, []string{"env", "app"}, names,
		"names must list every distinct label column in the section")
}

func TestStreamScan_MultiRegex_AND(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2}},
		{name: "env", value: "staging", streamIDs: []int64{3}},
		{name: "app", value: "foo", streamIDs: []int64{2, 4}},
		{name: "app", value: "bar", streamIDs: []int64{5}},
	})

	got, names := resolveToStreamIDs(t, obj, []*labels.Matcher{
		regexMatcher(t, "env", "^pr.*"),
		regexMatcher(t, "app", "^fo.*"),
	})
	require.Len(t, got, 1,
		"env=~^pr.* AND app=~^fo.* must intersect to stream 2, not union {1,2,4}")
	_, ok := got[2]
	require.True(t, ok, "stream 2 (env=prod AND app=foo) must be the sole survivor")
	_, has1 := got[1]
	require.False(t, has1, "stream 1 (env=prod only) must NOT appear in AND result")
	_, has4 := got[4]
	require.False(t, has4, "stream 4 (app=foo only) must NOT appear in AND result")
	_, has3 := got[3]
	require.False(t, has3, "stream 3 (env=staging) must NOT appear")
	_, has5 := got[5]
	require.False(t, has5, "stream 5 (app=bar) must NOT appear")

	require.ElementsMatch(t, []string{"env", "app"}, names,
		"names must list every distinct label column in the section")
}

func TestStreamScan_NotEqualMatcher_AcrossNames(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2}},
		{name: "env", value: "dev", streamIDs: []int64{3}},
		{name: "app", value: "foo", streamIDs: []int64{2, 4}},
		{name: "app", value: "bar", streamIDs: []int64{1, 5}},
	})

	notEqual, err := labels.NewMatcher(labels.MatchNotEqual, "app", "bar")
	require.NoError(t, err)

	got, _ := resolveToStreamIDs(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
		notEqual,
	})
	require.Len(t, got, 1,
		"env=prod AND app!=bar must yield exactly stream 2")
	_, ok := got[2]
	require.True(t, ok, "stream 2 (env=prod AND app=foo) must be the sole survivor")
	_, has1 := got[1]
	require.False(t, has1, "stream 1 (env=prod AND app=bar) must NOT appear — app!=bar rejects it")
}

func TestStreamScan_NotEqualMatcher_IncludesStreamsMissingLabel(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{name: "env", value: "prod", streamIDs: []int64{1, 2, 3}},
		{name: "app", value: "bar", streamIDs: []int64{2}},
	})

	notEqual, err := labels.NewMatcher(labels.MatchNotEqual, "app", "bar")
	require.NoError(t, err)

	got, _ := resolveToStreamIDs(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
		notEqual,
	})
	require.Len(t, got, 2,
		"env=prod AND app!=bar must keep streams missing app label (1 and 3)")
	_, has1 := got[1]
	require.True(t, has1, "stream 1 has env=prod and no app label, so app!=bar should include it")
	_, has3 := got[3]
	require.True(t, has3, "stream 3 has env=prod and no app label, so app!=bar should include it")
	_, has2 := got[2]
	require.False(t, has2, "stream 2 has app=bar, so app!=bar must exclude it")
}

func TestStreamScan_ObjectScopedStreamIDs(t *testing.T) {
	obj := buildLabelObject(t, []labelFixtureEntry{
		{objectPath: "/obj-a", name: "app", value: "foo", streamIDs: []int64{1}},
		{objectPath: "/obj-b", name: "app", value: "bar", streamIDs: []int64{1}},
	})

	res := resolveLabels(t, obj, []*labels.Matcher{
		equalMatcher(t, "app", "foo"),
	}, time.Unix(0, 0), time.Unix(0, 1<<62))
	got := res.MatchingStreamRefs
	require.Len(t, got, 1, "only /obj-a stream 1 matches app=foo")

	_, ok := got[streamRef{ObjectPath: "/obj-a", StreamID: 1}]
	require.True(t, ok, "object-scoped stream ref must be present")
	_, leaked := got[streamRef{ObjectPath: "/obj-b", StreamID: 1}]
	require.False(t, leaked, "same numeric stream ID in another object must not leak into the result")
	require.ElementsMatch(t, []string{"app"}, res.LabelColumnNames)
}

type labelFixtureEntry struct {
	objectPath string
	name       string
	value      string
	streamIDs  []int64
}

// buildLabelObject builds a data object holding a single postings section with
// the given label observations.
func buildLabelObject(tb testing.TB, entries []labelFixtureEntry) *dataobj.Object {
	tb.Helper()

	pb := postings.NewBuilder(nil, 0, 0, 1<<20)
	ts := time.Unix(0, 1000).UTC()
	for _, e := range entries {
		path := e.objectPath
		if path == "" {
			path = "/obj"
		}
		for _, sid := range e.streamIDs {
			pb.ObserveLabelPosting(postings.LabelObservation{
				ObjectPath:       path,
				SectionIndex:     0,
				ColumnName:       e.name,
				LabelValue:       e.value,
				StreamID:         sid,
				Timestamp:        ts,
				UncompressedSize: 1,
			})
		}
	}

	objBuilder := dataobj.NewBuilder(nil)
	require.NoError(tb, objBuilder.Append(pb))
	obj, closer, err := objBuilder.Flush()
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = closer.Close() })
	return obj
}

// resolveLabels runs the production label-resolution path over obj: a generic
// postings.Reader (kind == KindLabel) feeding a streamScan.
func resolveLabels(tb testing.TB, obj *dataobj.Object, matchers []*labels.Matcher, start, end time.Time) *streamScanResult {
	tb.Helper()

	acc := newStreamScan(matchers, start, end)
	for _, section := range obj.Sections() {
		if !postings.CheckSection(section) {
			continue
		}
		sec, err := postings.Open(tb.Context(), section)
		require.NoError(tb, err)

		cols, err := findPostingsColumnsByTypes(sec.Columns(),
			postings.ColumnTypeObjectPath,
			postings.ColumnTypeSectionIndex,
			postings.ColumnTypeColumnName,
			postings.ColumnTypeLabelValue,
			postings.ColumnTypeStreamIDBitmap,
			postings.ColumnTypeMinTimestamp,
			postings.ColumnTypeMaxTimestamp,
			postings.ColumnTypeKind,
		)
		require.NoError(tb, err)

		reader := postings.NewReader(postings.ReaderOptions{
			Columns: cols,
			Predicates: []postings.Predicate{
				postings.EqualPredicate{Column: cols[len(cols)-1], Value: scalar.NewInt64Scalar(int64(postings.KindLabel))},
			},
			Allocator: memory.DefaultAllocator,
		})
		require.NoError(tb, reader.Open(tb.Context()))
		for {
			rec, err := reader.Read(tb.Context(), 4096)
			if rec != nil && rec.NumRows() > 0 {
				require.NoError(tb, acc.accumulate(rec))
			}
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(tb, err)
		}
		_ = reader.Close()
	}
	return acc.finalize(tb.Context())
}

// resolveToStreamIDs resolves over a wide time window and returns the matching
// stream IDs plus the full distinct label-name set.
func resolveToStreamIDs(tb testing.TB, obj *dataobj.Object, matchers []*labels.Matcher) (map[int64]struct{}, []string) {
	tb.Helper()

	res := resolveLabels(tb, obj, matchers, time.Unix(0, 0), time.Unix(0, 1<<62))
	ids := make(map[int64]struct{}, len(res.MatchingStreamRefs))
	for ref := range res.MatchingStreamRefs {
		ids[ref.StreamID] = struct{}{}
	}
	return ids, res.LabelColumnNames
}
