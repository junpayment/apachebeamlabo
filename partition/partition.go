package partition

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func Do() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	type pair struct {
		Key   string
		Value int
	}
	lines := beam.CreateList(s, []pair{
		{"one", 1},
		{"one1", 11},
		{"two", 22},
		{"two2", 2},
		{"three", 3},
		{"three3", 33},
	})
	c := beam.Partition(s, 10, func(p pair) int {
		return int(float64(p.Value) / float64(10))
	}, lines)
	for _, v := range c {
		res := beam.ParDo(s, func(p pair) string {
			return fmt.Sprintf("%s: %v", p.Key, p.Value)
		}, v)
		textio.Write(s, "/dev/stdout", res)
	}
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
