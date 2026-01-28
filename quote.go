//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"unsafe"

	"simd/archsimd"
)

// =============================================================================
// Whitespace Handling
// =============================================================================

// isWhitespace reports whether b is a space or tab character.
func isWhitespace(b byte) bool {
	return b == ' ' || b == '\t'
}

// skipLeadingWhitespace returns the number of leading whitespace bytes.
func skipLeadingWhitespace(data []byte) int {
	for i := 0; i < len(data); i++ {
		if !isWhitespace(data[i]) {
			return i
		}
	}
	return len(data)
}

// =============================================================================
// Quote Detection
// =============================================================================

// isQuotedFieldStart checks if data starts with a quote, optionally after whitespace.
// Returns (isQuoted, quoteOffset) where quoteOffset is the position of the opening quote.
func isQuotedFieldStart(data []byte, trimLeadingSpace bool) (bool, int) {
	if len(data) == 0 {
		return false, 0
	}

	if data[0] == '"' {
		return true, 0
	}

	if trimLeadingSpace {
		offset := skipLeadingWhitespace(data)
		if offset > 0 && offset < len(data) && data[offset] == '"' {
			return true, offset
		}
	}

	return false, 0
}

// isEscapedQuote checks if the quote at position i is escaped (followed by another quote).
func isEscapedQuote(data []byte, i int) bool {
	return i+1 < len(data) && data[i+1] == '"'
}

// =============================================================================
// Closing Quote Search - Unified Dispatch
// =============================================================================

// findClosingQuote finds the closing quote in a quoted field.
// Returns the index of the closing quote, or -1 if not found.
// Handles escaped double quotes ("").
func findClosingQuote(data []byte, startAfterOpenQuote int) int {
	remaining := len(data) - startAfterOpenQuote
	if shouldUseSIMD(remaining) {
		return findClosingQuoteSIMD(data, startAfterOpenQuote)
	}
	return findClosingQuoteScalar(data, startAfterOpenQuote)
}

// =============================================================================
// Closing Quote Search - Scalar Implementation
// =============================================================================

// findClosingQuoteScalar finds the closing quote using scalar operations.
func findClosingQuoteScalar(data []byte, startAfterOpenQuote int) int {
	for i := startAfterOpenQuote; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		if isEscapedQuote(data, i) {
			i++ // Skip second quote of escape sequence (loop increments again)
			continue
		}
		return i
	}
	return -1
}

// =============================================================================
// Closing Quote Search - SIMD Implementation
// =============================================================================

// findClosingQuoteSIMD uses AVX-512 to find the closing quote in simdHalfChunk-byte chunks.
func findClosingQuoteSIMD(data []byte, startAfterOpenQuote int) int {
	quoteCmp := archsimd.BroadcastInt8x32('"')
	i := startAfterOpenQuote

	for i+simdHalfChunk <= len(data) {
		chunk := archsimd.LoadInt8x32((*[simdHalfChunk]int8)(unsafe.Pointer(&data[i])))
		mask := chunk.Equal(quoteCmp).ToBits()

		if mask == 0 {
			i += simdHalfChunk
			continue
		}

		result, newI, done := processQuoteMask(data, i, mask)
		if result >= 0 {
			return result
		}
		if done {
			break
		}
		i = newI
	}

	return findClosingQuoteScalar(data, i)
}

// processQuoteMask processes quote positions in a SIMD chunk mask.
// Returns (closingQuoteIdx, newPosition, shouldExitLoop).
func processQuoteMask(data []byte, chunkStart int, mask uint32) (int, int, bool) {
	for mask != 0 {
		pos := bits.TrailingZeros32(mask)

		// Boundary case: quote at last position of chunk
		if pos == simdHalfChunk-1 {
			newPos := chunkStart + simdHalfChunk
			if newPos < len(data) && data[newPos] == '"' {
				// Boundary double quote → skip both
				return -1, newPos + 1, false
			}
			// Closing quote at boundary
			return chunkStart + pos, chunkStart, false
		}

		// Check if next bit is also set (double quote "")
		nextBit := uint32(1) << (pos + 1)
		if mask&nextBit != 0 {
			// Double quote → clear both bits and continue
			mask &^= (uint32(1) << pos) | nextBit
			continue
		}

		// Single quote = closing quote
		return chunkStart + pos, chunkStart, false
	}

	return -1, chunkStart + simdHalfChunk, false
}
