package metastore

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/v3/pkg/dataobj"
	"github.com/grafana/loki/v3/pkg/dataobj/sections/postings"
)

func TestMatchSections_AND_Semantics(t *testing.T) {
	obj := buildBloomObject(t, []bloomFixtureEntry{
		{objectPath: "/objA", sectionIndex: 0, columnName: "env", values: []string{"prod"}},
		{objectPath: "/objA", sectionIndex: 0, columnName: "app", values: []string{"foo"}},
		{objectPath: "/objB", sectionIndex: 0, columnName: "env", values: []string{"prod"}},
		{objectPath: "/objB", sectionIndex: 0, columnName: "app", values: []string{"baz"}},
		{objectPath: "/objC", sectionIndex: 0, columnName: "env", values: []string{"dev"}},
		{objectPath: "/objC", sectionIndex: 0, columnName: "app", values: []string{"foo"}},
	})

	result, err := matchSectionsFromObject(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
		equalMatcher(t, "app", "foo"),
	})
	require.NoError(t, err)

	require.Len(t, result, 1, "exactly one section should match both env=prod AND app=foo")
	_, ok := result[SectionKey{ObjectPath: "/objA", SectionIdx: 0}]
	require.True(t, ok, "section A (/objA, 0) should be the sole match")
}

func TestMatchSections_EqualMatcherOnly_FilterApplied(t *testing.T) {
	obj := buildBloomObject(t, []bloomFixtureEntry{
		{objectPath: "/obj", sectionIndex: 0, columnName: "env", values: []string{"prod"}},
	})

	result, err := matchSectionsFromObject(t, obj, []*labels.Matcher{
		equalMatcher(t, "env", "prod"),
		regexMatcher(t, "app", ".*"),
	})
	require.NoError(t, err)

	require.Len(t, result, 1, "Equal matcher alone should match the section; Regex matcher is filtered out")
	_, ok := result[SectionKey{ObjectPath: "/obj", SectionIdx: 0}]
	require.True(t, ok)
}

type bloomFixtureEntry struct {
	objectPath   string
	sectionIndex int64
	columnName   string
	values       []string
}

func buildBloomObject(t *testing.T, entries []bloomFixtureEntry) *dataobj.Object {
	t.Helper()

	pb := postings.NewBuilder(nil, 0, 0, 1<<20)
	ts := time.Unix(0, 1000).UTC()
	for _, e := range entries {
		pb.PrepareBloomColumn(e.objectPath, e.sectionIndex, e.columnName, uint(len(e.values)))
		for streamIdx, v := range e.values {
			require.NoError(t, pb.ObserveBloomPosting(postings.BloomObservation{
				ObjectPath:       e.objectPath,
				SectionIndex:     e.sectionIndex,
				ColumnName:       e.columnName,
				Value:            v,
				StreamID:         int64(streamIdx + 1),
				Timestamp:        ts,
				UncompressedSize: 1,
			}))
		}
	}

	objBuilder := dataobj.NewBuilder(nil)
	require.NoError(t, objBuilder.Append(pb))
	obj, closer, err := objBuilder.Flush()
	require.NoError(t, err)
	t.Cleanup(func() { _ = closer.Close() })
	return obj
}

// matchSectionsFromObject runs the production bloom path over obj: a generic
// postings.Reader (kind == KindBloom) feeding matchSections.
func matchSectionsFromObject(tb testing.TB, obj *dataobj.Object, matchers []*labels.Matcher) (map[SectionKey]struct{}, error) {
	tb.Helper()

	var recs []arrow.RecordBatch
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
			postings.ColumnTypeBloomFilter,
			postings.ColumnTypeKind,
		)
		require.NoError(tb, err)

		reader := postings.NewReader(postings.ReaderOptions{
			Columns: cols,
			Predicates: []postings.Predicate{
				postings.EqualPredicate{Column: cols[len(cols)-1], Value: scalar.NewInt64Scalar(int64(postings.KindBloom))},
			},
			Allocator: memory.DefaultAllocator,
		})
		require.NoError(tb, reader.Open(tb.Context()))
		for {
			rec, err := reader.Read(tb.Context(), 4096)
			if rec != nil && rec.NumRows() > 0 {
				recs = append(recs, rec)
			}
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(tb, err)
		}
		_ = reader.Close()
	}
	return matchSections(tb.Context(), recs, matchers)
}

func equalMatcher(t *testing.T, name, value string) *labels.Matcher {
	t.Helper()
	m, err := labels.NewMatcher(labels.MatchEqual, name, value)
	require.NoError(t, err)
	return m
}

func regexMatcher(t *testing.T, name, value string) *labels.Matcher {
	t.Helper()
	m, err := labels.NewMatcher(labels.MatchRegexp, name, value)
	require.NoError(t, err)
	return m
}
