package combine

import (
	"context"
	"fmt"
	"log"
	"math"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/stats"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*fn)(nil)))
}

func Do() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	lines := beam.CreateList(s, []int{
		1, 20, 30,
	})
	c := beam.Combine(s, func(a, v int) int {
		return a + v
	}, lines)
	res := beam.ParDo(s, func(v int) string {
		return fmt.Sprintf("result: %d", v)
	}, c)
	textio.Write(s, "/dev/stdout", res)
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

type fn struct{}
type acc struct {
	Count, Sum int
}

func (r *fn) CreateAccumulator() acc {
	return acc{0, 0}
}
func (r *fn) AddInput(a acc, v int) acc {
	return acc{Count: a.Count + 1, Sum: a.Sum + v}
}
func (r *fn) MergeAccumulators(a, v acc) acc {
	return acc{Count: a.Count + v.Count, Sum: a.Sum + v.Sum}
}
func (r *fn) ExtractOutput(a acc) float64 {
	if a.Count == 0 {
		return math.NaN()
	}
	return float64(a.Sum) / float64(a.Count)
}

func Do2() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	lines := beam.CreateList(s, []int{
		1, 20, 30,
	})
	c := beam.Combine(s, &fn{}, lines)
	res := beam.ParDo(s, func(v float64) string {
		return fmt.Sprintf("result: %f", v)
	}, c)
	textio.Write(s, "/dev/stdout", res)
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}

func Do3() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	type pair struct {
		Key   string
		Value int
	}
	lines := beam.CreateList(s, []pair{
		{"one", 1},
		{"one", 11},
		{"two", 22},
		{"two", 2},
		{"three", 3},
		{"three", 33},
	})
	t := beam.ParDo(s, func(p pair) (string, float64) {
		return p.Key, float64(p.Value)
	}, lines)
	c2 := stats.MeanPerKey(s, t)
	res := beam.ParDo(s, func(key string, r float64) string {
		return fmt.Sprintf("%s: %v", key, r)
	}, c2)
	textio.Write(s, "/dev/stdout", res)
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
