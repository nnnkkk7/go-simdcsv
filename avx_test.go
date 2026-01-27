//go:build goexperiment.simd && amd64

package simdcsv

import (
	"fmt"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	fmt.Fprintf(os.Stderr, "simdcsv: useAVX512=%v\n", useAVX512)
	os.Exit(m.Run())
}
