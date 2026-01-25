//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"simd/archsimd"
	"unsafe"
)

// skipLeadingWhitespace returns the number of leading whitespace bytes (space or tab).
func skipLeadingWhitespace(data []byte) int {
	i := 0
	for i < len(data) && (data[i] == ' ' || data[i] == '\t') {
		i++
	}
	return i
}

// isQuotedFieldStart checks if data starts with a quote, optionally after whitespace.
// Returns (isQuoted, quoteOffset) where quoteOffset is the position of the opening quote.
func isQuotedFieldStart(data []byte, trimLeadingSpace bool) (bool, int) {
	if len(data) == 0 {
		return false, 0
	}

	// Direct quote at start
	if data[0] == '"' {
		return true, 0
	}

	// Check for whitespace followed by quote if trimming is enabled
	if trimLeadingSpace {
		offset := skipLeadingWhitespace(data)
		if offset > 0 && offset < len(data) && data[offset] == '"' {
			return true, offset
		}
	}

	return false, 0
}

// findClosingQuote finds the closing quote in a quoted field.
// Returns the index of the closing quote, or -1 if not found.
// Handles escaped double quotes ("").
// Dispatches to SIMD or scalar implementation based on CPU support and data size.
func findClosingQuote(data []byte, startAfterOpenQuote int) int {
	remaining := len(data) - startAfterOpenQuote
	// Use SIMD for data >= simdMinThreshold bytes, otherwise scalar is faster
	if useAVX512 && remaining >= simdMinThreshold {
		return findClosingQuoteSIMD(data, startAfterOpenQuote)
	}
	return findClosingQuoteScalar(data, startAfterOpenQuote)
}

// findClosingQuoteScalar is the scalar implementation of findClosingQuote.
func findClosingQuoteScalar(data []byte, startAfterOpenQuote int) int {
	i := startAfterOpenQuote
	for i < len(data) {
		if data[i] == '"' {
			// Check for escaped quote
			if i+1 < len(data) && data[i+1] == '"' {
				i += 2
				continue
			}
			// This is the closing quote
			return i
		}
		i++
	}
	return -1
}

// findClosingQuoteSIMD uses SIMD to find the closing quote.
// It searches for quote characters in simdHalfChunk-byte chunks using AVX-512.
func findClosingQuoteSIMD(data []byte, startAfterOpenQuote int) int {
	quoteCmp := archsimd.BroadcastInt8x32('"')
	i := startAfterOpenQuote

	// Process simdHalfChunk-byte chunks
	for i+simdHalfChunk <= len(data) {
		chunk := archsimd.LoadInt8x32((*[simdHalfChunk]int8)(unsafe.Pointer(&data[i])))
		mask := chunk.Equal(quoteCmp).ToBits()

		if mask != 0 {
			// Found at least one quote in this chunk
			for mask != 0 {
				// Find the position of the first quote
				pos := bits.TrailingZeros32(mask)
				absPos := i + pos

				// Check for escaped quote (double quote)
				if absPos+1 < len(data) && data[absPos+1] == '"' {
					// This is an escaped quote, skip both quotes
					// Clear this bit and the next (if in same chunk)
					mask &= ^(uint32(1) << pos)
					if pos+1 < simdHalfChunk {
						mask &= ^(uint32(1) << (pos + 1))
					}
					// If next quote is in the next chunk, we need to skip it.
					// Using goto here for performance: it allows us to skip the normal
					// i += simdHalfChunk increment and immediately continue with the
					// already-adjusted i value after handling boundary double quotes.
					if pos == simdHalfChunk-1 {
						i += simdHalfChunk
						// Skip the first quote of the next iteration
						if i < len(data) && data[i] == '"' {
							i++
						}
						goto continueLoop
					}
					continue
				}
				// This is the closing quote
				return absPos
			}
		}
		i += simdHalfChunk
	continueLoop:
	}

	// Process remaining bytes with scalar implementation
	return findClosingQuoteScalar(data, i)
}

// extractQuotedContent extracts content from a quoted field, handling unescaping.
// data should start from the opening quote.
// Returns the unescaped content between quotes.
func extractQuotedContent(data []byte, closingQuoteIdx int) string {
	if closingQuoteIdx <= 1 {
		return ""
	}
	return string(data[1:closingQuoteIdx])
}
