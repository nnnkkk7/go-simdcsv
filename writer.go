//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bufio"
	"io"
	"math/bits"
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
	// Use SIMD for ASCII delimiters and fields meeting size threshold
	if shouldUseSIMD(len(field)) && w.Comma >= 0 && w.Comma < 128 {
		return w.fieldNeedsQuotesSIMD(field)
	}
	return w.fieldNeedsQuotesScalar(field)
}

// fieldNeedsQuotesScalar checks for special characters using scalar iteration.
func (w *Writer) fieldNeedsQuotesScalar(field string) bool {
	for _, c := range field {
		if c == w.Comma || c == '\n' || c == '\r' || c == '"' {
			return true
		}
	}
	return false
}

// fieldNeedsQuotesSIMD uses AVX-512 SIMD to detect special characters requiring quoting.
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

	// Process remaining bytes (< 64 bytes, scalar is sufficient)
	for ; offset < len(data); offset++ {
		c := data[offset]
		if c == byte(w.Comma) || c == '\n' || c == '\r' || c == '"' {
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
	if shouldUseSIMD(len(field)) {
		return w.writeQuotedFieldSIMD(field)
	}
	return w.writeQuotedFieldScalar(field)
}

// writeQuotedFieldScalar escapes quotes using scalar iteration.
func (w *Writer) writeQuotedFieldScalar(field string) error {
	for _, c := range field {
		if c == '"' {
			if _, err := w.w.WriteString(`""`); err != nil {
				return err
			}
		} else {
			if _, err := w.w.WriteRune(c); err != nil {
				return err
			}
		}
	}
	return w.w.WriteByte('"')
}

// writeQuotedFieldSIMD escapes quotes using AVX-512 SIMD to find quote positions.
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

	// Process remaining bytes (< 64 bytes, scalar is sufficient)
	for ; offset < len(data); offset++ {
		if data[offset] == '"' {
			if _, err := w.w.WriteString(field[lastWritten : offset+1]); err != nil {
				return err
			}
			if err := w.w.WriteByte('"'); err != nil {
				return err
			}
			lastWritten = offset + 1
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
