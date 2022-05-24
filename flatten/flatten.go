package flatten

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
		{"one", 11},
		{"two", 22},
		{"two", 2},
		{"three", 3},
		{"three", 33},
	})
	c := beam.Flatten(s, lines, lines, lines, lines)
	res := beam.ParDo(s, func(p pair) string {
		return fmt.Sprintf("%s: %v", p.Key, p.Value)
	}, c)
	textio.Write(s, "/dev/stdout", res)
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
