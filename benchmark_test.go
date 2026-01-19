//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bytes"
	"encoding/csv"
	"io"
	"testing"
)

// =============================================================================
// ReadAll Benchmarks - Simple CSV
// =============================================================================

func BenchmarkReadAll_Simple_1K_Stdlib(b *testing.B) {
	data := generateSimpleCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Simple_1K_SIMD(b *testing.B) {
	data := generateSimpleCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Simple_10K_Stdlib(b *testing.B) {
	data := generateSimpleCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Simple_10K_SIMD(b *testing.B) {
	data := generateSimpleCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Simple_100K_Stdlib(b *testing.B) {
	data := generateSimpleCSV(100000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Simple_100K_SIMD(b *testing.B) {
	data := generateSimpleCSV(100000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

// =============================================================================
// ReadAll Benchmarks - Quoted CSV
// =============================================================================

func BenchmarkReadAll_Quoted_1K_Stdlib(b *testing.B) {
	data := generateQuotedCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Quoted_1K_SIMD(b *testing.B) {
	data := generateQuotedCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Quoted_10K_Stdlib(b *testing.B) {
	data := generateQuotedCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Quoted_10K_SIMD(b *testing.B) {
	data := generateQuotedCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

// =============================================================================
// ReadAll Benchmarks - Mixed CSV
// =============================================================================

func BenchmarkReadAll_Mixed_1K_Stdlib(b *testing.B) {
	data := generateMixedCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Mixed_1K_SIMD(b *testing.B) {
	data := generateMixedCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Mixed_10K_Stdlib(b *testing.B) {
	data := generateMixedCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_Mixed_10K_SIMD(b *testing.B) {
	data := generateMixedCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

// =============================================================================
// ReadAll Benchmarks - Escaped Quotes CSV
// =============================================================================

func BenchmarkReadAll_EscapedQuotes_1K_Stdlib(b *testing.B) {
	data := generateEscapedQuotesCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_EscapedQuotes_1K_SIMD(b *testing.B) {
	data := generateEscapedQuotesCSV(1000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_EscapedQuotes_10K_Stdlib(b *testing.B) {
	data := generateEscapedQuotesCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

func BenchmarkReadAll_EscapedQuotes_10K_SIMD(b *testing.B) {
	data := generateEscapedQuotesCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		_, _ = reader.ReadAll()
	}
}

// =============================================================================
// Record-by-Record Read Benchmarks
// =============================================================================

func BenchmarkRead_RecordByRecord_10K_Stdlib(b *testing.B) {
	data := generateSimpleCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := csv.NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		for {
			_, err := reader.Read()
			if err == io.EOF {
				break
			}
		}
	}
}

func BenchmarkRead_RecordByRecord_10K_SIMD(b *testing.B) {
	data := generateSimpleCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		reader := NewReader(bytes.NewReader(data))
		reader.FieldsPerRecord = -1
		for {
			_, err := reader.Read()
			if err == io.EOF {
				break
			}
		}
	}
}

// =============================================================================
// ParseBytes Benchmark (simdcsv-specific zero-copy API)
// =============================================================================

func BenchmarkParseBytes_Simple_10K(b *testing.B) {
	data := generateSimpleCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		_, _ = ParseBytes(data, ',')
	}
}

func BenchmarkParseBytes_Quoted_10K(b *testing.B) {
	data := generateQuotedCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		_, _ = ParseBytes(data, ',')
	}
}

func BenchmarkParseBytes_Mixed_10K(b *testing.B) {
	data := generateMixedCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		_, _ = ParseBytes(data, ',')
	}
}

func BenchmarkParseBytes_EscapedQuotes_10K(b *testing.B) {
	data := generateEscapedQuotesCSV(10000, 10)
	b.SetBytes(int64(len(data)))
	for b.Loop() {
		_, _ = ParseBytes(data, ',')
	}
}
