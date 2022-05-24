package groupbykey

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*pair)(nil)).Elem())
}

type pair struct {
	Key   string
	Value string
}

func Do() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	lines := beam.CreateList(s, []pair{
		{"one", "1"},
		{"two", "2"},
		{"three", "3"},
	})
	lines2 := beam.CreateList(s, []pair{
		{"one", "uno"},
		{"one", "ichi"},
		{"two", "dos"},
		{"two", "ni"},
		{"three", "tres"},
		{"three", "san"},
	})
	fn := func(p pair) (string, string) {
		return p.Key, p.Value
	}
	numbers := beam.ParDo(s, fn, lines)
	languages := beam.ParDo(s, fn, lines2)
	results := beam.CoGroupByKey(s, numbers, languages)
	res1 := beam.ParDo(s, func(key string, itrnumber, itrlanguage func(*string) bool) string {
		var tmp string
		var nums, langs []string
		for itrnumber(&tmp) {
			nums = append(nums, tmp)
		}
		for itrlanguage(&tmp) {
			langs = append(langs, tmp)
		}
		return fmt.Sprintf("%s: %v: %v", key, nums, langs)
	}, results)
	results2 := beam.GroupByKey(s, languages)
	res2 := beam.ParDo(s, func(key string, itr func(*string) bool) string {
		var tmp string
		var langs []string
		for itr(&tmp) {
			langs = append(langs, tmp)
		}
		return fmt.Sprintf("%s: %v", key, langs)
	}, results2)
	textio.Write(s, "/dev/stdout", res1)
	textio.Write(s, "/dev/stdout", res2)
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
