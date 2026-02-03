//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bufio"
	"io"
	"math/bits"
	"strings"
	"unsafe"

	"simd/archsimd"
)

// Writer writes records using CSV encoding.
//
// Records are terminated by a newline and use ',' as the field delimiter by default.
// The exported fields can be changed before the first call to Write or WriteAll.
//
// Writes are buffered; call Flush to ensure data reaches the underlying io.Writer.
// Check Error for any errors that occurred during Write or Flush.
type Writer struct {
	Comma   rune // Field delimiter (set to ',' by NewWriter)
	UseCRLF bool // Use \r\n as line terminator instead of \n

	w   *bufio.Writer
	err error
}

// NewWriter returns a new Writer that writes to w.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		Comma: ',',
		w:     bufio.NewWriter(w),
	}
}

// Write writes a single CSV record with necessary quoting.
// Writes are buffered; call Flush to ensure output reaches the underlying Writer.
func (w *Writer) Write(record []string) error {
	if w.err != nil {
		return w.err
	}

	for i, field := range record {
		if i > 0 {
			if _, w.err = w.w.WriteRune(w.Comma); w.err != nil {
				return w.err
			}
		}
		if w.err = w.writeField(field); w.err != nil {
			return w.err
		}
	}

	return w.writeLineEnding()
}

// writeField writes a single field, quoting if necessary.
func (w *Writer) writeField(field string) error {
	if w.fieldNeedsQuotes(field) {
		return w.writeQuotedField(field)
	}
	_, err := w.w.WriteString(field)
	return err
}

// writeLineEnding writes \r\n or \n based on UseCRLF setting.
func (w *Writer) writeLineEnding() error {
	if w.UseCRLF {
		_, w.err = w.w.WriteString("\r\n")
	} else {
		w.err = w.w.WriteByte('\n')
	}
	return w.err
}

// WriteAll writes multiple records and calls Flush.
func (w *Writer) WriteAll(records [][]string) error {
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.Flush()
}

// Flush writes buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	w.err = w.w.Flush()
	return w.err
}

// Error returns any error from a previous Write or Flush.
func (w *Writer) Error() error {
	return w.err
}

// writerSIMDMinSize is the minimum field size for SIMD benefit in writeQuotedField.
const writerSIMDMinSize = 16

// writerSIMDCheckThreshold is the minimum size for SIMD benefit in fieldNeedsQuotes.
// Higher than writerSIMDMinSize because checking has more overhead than writing.
const writerSIMDCheckThreshold = 64

// fieldNeedsQuotes reports whether field requires quoting.
// Dispatches to SIMD or scalar based on CPU support and field size.
func (w *Writer) fieldNeedsQuotes(field string) bool {
	if len(field) == 0 {
		return false
	}
	// Leading whitespace always requires quoting
	if field[0] == ' ' || field[0] == '\t' {
		return true
	}
	// Use SIMD only for larger fields where the overhead is justified
	if useAVX512 && len(field) >= writerSIMDCheckThreshold && w.Comma >= 0 && w.Comma < 128 {
		return w.fieldNeedsQuotesSIMD(field)
	}
	return w.fieldNeedsQuotesScalar(field)
}

// fieldNeedsQuotesScalar checks for special characters using direct byte iteration.
// This is faster than strings.ContainsAny for short strings due to charset building overhead.
func (w *Writer) fieldNeedsQuotesScalar(field string) bool {
	// For ASCII comma (common case), use direct byte comparison
	if w.Comma < 128 {
		comma := byte(w.Comma)
		for i := 0; i < len(field); i++ {
			c := field[i]
			if c == comma || c == '\n' || c == '\r' || c == '"' {
				return true
			}
		}
		return false
	}
	// For non-ASCII comma, fall back to rune iteration
	for _, c := range field {
		if c == w.Comma || c == '\n' || c == '\r' || c == '"' {
			return true
		}
	}
	return false
}

// fieldNeedsQuotesSIMD uses AVX-512 SIMD to detect special characters requiring quoting.
// Handles any field size >= writerSIMDMinSize using padded operations for partial chunks.
func (w *Writer) fieldNeedsQuotesSIMD(field string) bool {
	data := unsafe.Slice(unsafe.StringData(field), len(field))
	int8Data := bytesToInt8Slice(data)

	commaCmp := cachedSepCmp[w.Comma]

	// Process 64-byte chunks using AVX-512
	offset := 0
	for offset+simdChunkSize <= len(data) {
		chunk := archsimd.LoadInt8x64Slice(int8Data[offset : offset+simdChunkSize])

		commaMask := chunk.Equal(commaCmp).ToBits()
		newlineMask := chunk.Equal(cachedNlCmp).ToBits()
		crMask := chunk.Equal(cachedCrCmp).ToBits()
		quoteMask := chunk.Equal(cachedQuoteCmp).ToBits()

		if commaMask|newlineMask|crMask|quoteMask != 0 {
			return true
		}
		offset += simdChunkSize
	}

	// Process remaining bytes using SIMD with partial load
	if offset < len(data) {
		remaining := data[offset:]
		chunk := archsimd.LoadInt8x64SlicePart(bytesToInt8Slice(remaining))

		commaMask := chunk.Equal(commaCmp).ToBits()
		newlineMask := chunk.Equal(cachedNlCmp).ToBits()
		crMask := chunk.Equal(cachedCrCmp).ToBits()
		quoteMask := chunk.Equal(cachedQuoteCmp).ToBits()

		// Mask out bits beyond valid data
		validBits := len(remaining)
		mask := (uint64(1) << validBits) - 1
		if (commaMask|newlineMask|crMask|quoteMask)&mask != 0 {
			return true
		}
	}
	return false
}

// writeQuotedField writes a field surrounded by quotes, escaping internal quotes.
func (w *Writer) writeQuotedField(field string) error {
	if err := w.w.WriteByte('"'); err != nil {
		return err
	}
	// Use SIMD for fields that benefit from parallel quote detection
	if useAVX512 && len(field) >= writerSIMDMinSize {
		return w.writeQuotedFieldSIMD(field)
	}
	return w.writeQuotedFieldScalar(field)
}

// writeQuotedFieldScalar escapes quotes using optimized batch writing.
// Instead of writing character by character, it finds quotes using IndexByte
// and writes chunks between quotes in single WriteString calls.
func (w *Writer) writeQuotedFieldScalar(field string) error {
	lastWritten := 0
	for i := 0; i < len(field); {
		// Find next quote position from current offset
		idx := strings.IndexByte(field[i:], '"')
		if idx == -1 {
			break // No more quotes in remaining string
		}
		quotePos := i + idx
		// Write content up to and including the quote, then add escape quote
		if _, err := w.w.WriteString(field[lastWritten : quotePos+1]); err != nil {
			return err
		}
		if err := w.w.WriteByte('"'); err != nil {
			return err
		}
		lastWritten = quotePos + 1
		i = lastWritten
	}
	// Write remaining content after last quote (or entire field if no quotes)
	if lastWritten < len(field) {
		if _, err := w.w.WriteString(field[lastWritten:]); err != nil {
			return err
		}
	}
	return w.w.WriteByte('"')
}

// writeQuotedFieldSIMD escapes quotes using AVX-512 SIMD to find quote positions.
// Handles any field size >= writerSIMDMinSize using padded operations for partial chunks.
func (w *Writer) writeQuotedFieldSIMD(field string) error {
	data := unsafe.Slice(unsafe.StringData(field), len(field))
	int8Data := bytesToInt8Slice(data)

	offset := 0
	lastWritten := 0

	// Process 64-byte chunks using AVX-512
	for offset+simdChunkSize <= len(data) {
		chunk := archsimd.LoadInt8x64Slice(int8Data[offset : offset+simdChunkSize])
		mask := chunk.Equal(cachedQuoteCmp).ToBits()

		for mask != 0 {
			pos := bits.TrailingZeros64(mask)
			quotePos := offset + pos

			// Write content up to and including the quote, then add escape quote
			if _, err := w.w.WriteString(field[lastWritten : quotePos+1]); err != nil {
				return err
			}
			if err := w.w.WriteByte('"'); err != nil {
				return err
			}

			lastWritten = quotePos + 1
			mask &= ^(uint64(1) << pos)
		}
		offset += simdChunkSize
	}

	// Process remaining bytes using SIMD with partial load
	if offset < len(data) {
		remaining := data[offset:]
		chunk := archsimd.LoadInt8x64SlicePart(bytesToInt8Slice(remaining))
		mask := chunk.Equal(cachedQuoteCmp).ToBits()

		// Mask out bits beyond valid data
		validBits := len(remaining)
		validMask := (uint64(1) << validBits) - 1
		mask &= validMask

		for mask != 0 {
			pos := bits.TrailingZeros64(mask)
			quotePos := offset + pos

			// Write content up to and including the quote, then add escape quote
			if _, err := w.w.WriteString(field[lastWritten : quotePos+1]); err != nil {
				return err
			}
			if err := w.w.WriteByte('"'); err != nil {
				return err
			}

			lastWritten = quotePos + 1
			mask &= ^(uint64(1) << pos)
		}
	}

	// Write remaining content and closing quote
	if lastWritten < len(field) {
		if _, err := w.w.WriteString(field[lastWritten:]); err != nil {
			return err
		}
	}
	return w.w.WriteByte('"')
}
