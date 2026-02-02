//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"strings"
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

// =============================================================================
// findClosingQuote Benchmarks
// =============================================================================

func BenchmarkFindClosingQuote_Short(b *testing.B) {
	input := []byte(`"hello world"`)
	for b.Loop() {
		findClosingQuote(input, 1)
	}
}

func BenchmarkFindClosingQuote_Long(b *testing.B) {
	input := []byte(`"` + strings.Repeat("abcdefgh", 100) + `"`)
	for b.Loop() {
		findClosingQuote(input, 1)
	}
}

func BenchmarkFindClosingQuote_LongScalar(b *testing.B) {
	input := []byte(`"` + strings.Repeat("abcdefgh", 100) + `"`)
	for b.Loop() {
		findClosingQuoteScalar(input, 1)
	}
}

func BenchmarkFindClosingQuote_LongWithEscapes(b *testing.B) {
	input := []byte(`"` + strings.Repeat(`a""b`, 50) + `"`)
	for b.Loop() {
		findClosingQuote(input, 1)
	}
}

// =============================================================================
// fieldNeedsQuotes Benchmarks
// =============================================================================

func BenchmarkFieldNeedsQuotes_Short(b *testing.B) {
	w := NewWriter(nil)
	field := "hello,world"
	for b.Loop() {
		w.fieldNeedsQuotes(field)
	}
}

func BenchmarkFieldNeedsQuotes_Long(b *testing.B) {
	w := NewWriter(nil)
	field := strings.Repeat("abcdefgh", 100)
	for b.Loop() {
		w.fieldNeedsQuotes(field)
	}
}

func BenchmarkFieldNeedsQuotes_LongScalar(b *testing.B) {
	w := NewWriter(nil)
	field := strings.Repeat("abcdefgh", 100)
	for b.Loop() {
		w.fieldNeedsQuotesScalar(field)
	}
}

func BenchmarkFieldNeedsQuotes_LongWithSpecial(b *testing.B) {
	w := NewWriter(nil)
	field := strings.Repeat("abcdefgh", 100) + ","
	for b.Loop() {
		w.fieldNeedsQuotes(field)
	}
}

func BenchmarkWriteQuotedField_Long(b *testing.B) {
	field := strings.Repeat("a", 50) + `"` + strings.Repeat("b", 50)
	for b.Loop() {
		var buf bytes.Buffer
		w := NewWriter(&buf)
		_ = w.Write([]string{field})
		_ = w.Flush()
	}
}

// =============================================================================
// scanBuffer Benchmarks
// =============================================================================

func BenchmarkGenerateMasks(b *testing.B) {
	data := make([]byte, 64)
	copy(data, []byte(`"field1","field2","field3","field4","field5","field6","fie"`))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateMasks(data, ',')
	}
}

func BenchmarkGenerateMasksPadded(b *testing.B) {
	sizes := []int{1, 16, 32, 48, 63}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				if i%2 == 0 {
					data[i] = ','
				} else {
					data[i] = 'a'
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				generateMasksPadded(data, ',')
			}
		})
	}
}

func BenchmarkScanBuffer(b *testing.B) {
	sizes := []int{64, 1024, 64 * 1024, 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Create realistic CSV-like data
			data := make([]byte, size)
			for i := range data {
				switch i % 10 {
				case 3, 7:
					data[i] = ','
				case 9:
					data[i] = '\n'
				default:
					data[i] = 'a' + byte(i%26)
				}
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				scanBuffer(data, ',')
			}
		})
	}
}

// =============================================================================
// parseBuffer Benchmarks
// =============================================================================

func BenchmarkParseBuffer(b *testing.B) {
	// Generate test data: 10000 rows of "field1,field2,field3\n"
	numRows := 10000
	var data []byte
	for i := 0; i < numRows; i++ {
		data = append(data, []byte("field1,field2,field3\n")...)
	}

	// Pre-compute masks
	chunkCount := (len(data) + 63) / 64
	sepMasks := make([]uint64, chunkCount)
	nlMasks := make([]uint64, chunkCount)

	for i := 0; i < len(data); i++ {
		chunkIdx := i / 64
		bitPos := i % 64
		if data[i] == ',' {
			sepMasks[chunkIdx] |= 1 << bitPos
		} else if data[i] == '\n' {
			nlMasks[chunkIdx] |= 1 << bitPos
		}
	}

	sr := &scanResult{
		quoteMasks:     make([]uint64, chunkCount),
		separatorMasks: sepMasks,
		newlineMasks:   nlMasks,
		chunkCount:     chunkCount,
		lastChunkBits:  len(data) % 64,
	}
	if sr.lastChunkBits == 0 {
		sr.lastChunkBits = 64
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = parseBuffer(data, sr)
	}
}
