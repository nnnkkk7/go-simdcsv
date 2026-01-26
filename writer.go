//go:build goexperiment.simd && amd64

package simdcsv

import (
	"bufio"
	"io"
	"math/bits"
	"simd/archsimd"
	"unsafe"
)

// Writer writes records using CSV encoding.
//
// As returned by NewWriter, a Writer writes records terminated by a
// newline and uses ',' as the field delimiter. The exported fields can
// be changed to customize the details before the first call to Write or WriteAll.
//
// Comma is the field delimiter.
//
// If UseCRLF is true, the Writer ends each output line with \r\n instead of \n.
//
// The writes of individual records are buffered.
// After all data has been written, the client should call the
// Flush method to guarantee all data has been forwarded to
// the underlying io.Writer. Any errors that occurred should
// be checked by calling the Error method.
type Writer struct {
	Comma   rune // Field delimiter (set to ',' by NewWriter)
	UseCRLF bool // True to use \r\n as the line terminator

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

// Write writes a single CSV record to w along with any necessary quoting.
// A record is a slice of strings with each string being one field.
// Writes are buffered, so Flush must eventually be called to ensure
// that the record is written to the underlying io.Writer.
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
		w.err = w.writeField(field)
		if w.err != nil {
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

// writeLineEnding writes the appropriate line ending.
func (w *Writer) writeLineEnding() error {
	if w.UseCRLF {
		_, w.err = w.w.WriteString("\r\n")
	} else {
		w.err = w.w.WriteByte('\n')
	}
	return w.err
}

// WriteAll writes multiple CSV records to w using Write and then calls Flush,
// returning any error from the Flush.
func (w *Writer) WriteAll(records [][]string) error {
	for _, record := range records {
		if err := w.Write(record); err != nil {
			return err
		}
	}
	return w.Flush()
}

// Flush writes any buffered data to the underlying io.Writer.
// To check if an error occurred during Flush, call Error.
func (w *Writer) Flush() error {
	w.err = w.w.Flush()
	return w.err
}

// Error reports any error that has occurred during a previous Write or Flush.
func (w *Writer) Error() error {
	return w.err
}

// fieldNeedsQuotes reports whether field needs to be quoted.
// Dispatches to SIMD or scalar implementation based on CPU support and field size.
func (w *Writer) fieldNeedsQuotes(field string) bool {
	if len(field) == 0 {
		return false
	}
	// Check leading whitespace first (common case)
	if field[0] == ' ' || field[0] == '\t' {
		return true
	}
	// Use SIMD for ASCII comma and fields meeting threshold
	if shouldUseSIMD(len(field)) && w.Comma < 128 {
		return w.fieldNeedsQuotesSIMD(field)
	}
	return w.fieldNeedsQuotesScalar(field)
}

// fieldNeedsQuotesScalar is the scalar implementation.
func (w *Writer) fieldNeedsQuotesScalar(field string) bool {
	for _, c := range field {
		if c == w.Comma || c == '\n' || c == '\r' || c == '"' {
			return true
		}
	}
	return false
}

// fieldNeedsQuotesSIMD uses SIMD to check if field needs quoting.
// Searches for comma, newline, carriage return, and quote characters.
func (w *Writer) fieldNeedsQuotesSIMD(field string) bool {
	// Get byte slice from string without copying (safe for read-only access)
	data := unsafe.Slice(unsafe.StringData(field), len(field))

	// Broadcast comparison values
	commaCmp := archsimd.BroadcastInt8x32(int8(w.Comma))
	nlCmp := archsimd.BroadcastInt8x32('\n')
	crCmp := archsimd.BroadcastInt8x32('\r')
	quoteCmp := archsimd.BroadcastInt8x32('"')

	i := 0
	// Process 32-byte chunks
	for i+32 <= len(data) {
		chunk := archsimd.LoadInt8x32((*[32]int8)(unsafe.Pointer(&data[i])))

		// Check for any special character
		commaMask := chunk.Equal(commaCmp).ToBits()
		nlMask := chunk.Equal(nlCmp).ToBits()
		crMask := chunk.Equal(crCmp).ToBits()
		quoteMask := chunk.Equal(quoteCmp).ToBits()

		// If any special character found, needs quoting
		if commaMask|nlMask|crMask|quoteMask != 0 {
			return true
		}
		i += 32
	}

	// Process remaining bytes with scalar
	for ; i < len(data); i++ {
		c := data[i]
		if c == byte(w.Comma) || c == '\n' || c == '\r' || c == '"' {
			return true
		}
	}
	return false
}

// writeQuotedField writes a field that needs quoting.
func (w *Writer) writeQuotedField(field string) error {
	if err := w.w.WriteByte('"'); err != nil {
		return err
	}

	if shouldUseSIMD(len(field)) {
		return w.writeQuotedFieldSIMD(field)
	}
	return w.writeQuotedFieldScalar(field)
}

// writeQuotedFieldScalar is the scalar implementation.
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

// writeQuotedFieldSIMD uses SIMD to find quotes and write efficiently.
func (w *Writer) writeQuotedFieldSIMD(field string) error {
	data := unsafe.Slice(unsafe.StringData(field), len(field))
	quoteCmp := archsimd.BroadcastInt8x32('"')

	i := 0
	lastWritten := 0

	// Process 32-byte chunks
	for i+32 <= len(data) {
		chunk := archsimd.LoadInt8x32((*[32]int8)(unsafe.Pointer(&data[i])))
		mask := chunk.Equal(quoteCmp).ToBits()

		if mask != 0 {
			// Found quotes in this chunk - process them
			for mask != 0 {
				pos := bits.TrailingZeros32(mask)
				absPos := i + pos

				// Write everything up to and including this quote
				if _, err := w.w.WriteString(field[lastWritten : absPos+1]); err != nil {
					return err
				}
				// Write the extra quote for escaping
				if err := w.w.WriteByte('"'); err != nil {
					return err
				}
				lastWritten = absPos + 1
				mask &= ^(uint32(1) << pos)
			}
		}
		i += 32
	}

	// Process remaining bytes
	for ; i < len(data); i++ {
		if data[i] == '"' {
			if _, err := w.w.WriteString(field[lastWritten : i+1]); err != nil {
				return err
			}
			if err := w.w.WriteByte('"'); err != nil {
				return err
			}
			lastWritten = i + 1
		}
	}

	// Write any remaining content
	if lastWritten < len(field) {
		if _, err := w.w.WriteString(field[lastWritten:]); err != nil {
			return err
		}
	}

	return w.w.WriteByte('"')
}
