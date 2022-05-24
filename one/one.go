package one

import (
	"context"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*StringLength)(nil)))
	beam.RegisterFunction(Itoa)
}

type StringLength struct{}

func (r *StringLength) ProcessElement(str string, emit func(int)) {
	emit(len(str))
}

func Itoa(l int, emit func(string)) {
	emit(strconv.Itoa(l))
}

func do(str string) string {
	return strings.Join([]string{"yeah", str}, ":")
}

func Do() {
	beam.Init()
	p, s := beam.NewPipelineWithRoot()
	lines := beam.CreateList(s, []string{"one", "two", "three"})
	done := beam.ParDo(s, do, lines)
	upper := beam.ParDo(s, strings.ToUpper, lines)
	counted := beam.ParDo(s, &StringLength{}, lines)
	countedFn := beam.ParDo(s, func(line string) int {
		return len(line)
	}, lines)
	countedStr := beam.ParDo(s, Itoa, counted)
	countedStr2 := beam.ParDo(s, Itoa, countedFn)
	textio.Write(s, "/dev/stdout", done)
	textio.Write(s, "/dev/stdout", upper)
	textio.Write(s, "/dev/stdout", countedStr)
	textio.Write(s, "/dev/stdout", countedStr2)
	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Fatalf("Failed to execute job: %v", err)
	}
}
