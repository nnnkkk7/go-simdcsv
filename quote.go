//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"strings"
	"unsafe"

	"simd/archsimd"
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
	if shouldUseSIMD(remaining) {
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

// =============================================================================
// Double Quote Unescaping
// =============================================================================

// unescapeDoubleQuotes converts double quotes ("") to single quotes (").
// Dispatches to SIMD or scalar implementation based on CPU support and string size.
func unescapeDoubleQuotes(s string) string {
	if shouldUseSIMD(len(s)) {
		return unescapeDoubleQuotesSIMD(s)
	}
	return unescapeDoubleQuotesScalar(s)
}

// unescapeDoubleQuotesScalar is the scalar implementation.
func unescapeDoubleQuotesScalar(s string) string {
	// Fast path: no double quotes
	if !strings.Contains(s, `""`) {
		return s
	}
	return strings.ReplaceAll(s, `""`, `"`)
}

// unescapeDoubleQuotesSIMD uses SIMD to find double quotes and unescape them.
func unescapeDoubleQuotesSIMD(s string) string {
	if len(s) < simdChunkSize {
		return unescapeDoubleQuotesScalar(s)
	}
	data := unsafe.Slice(unsafe.StringData(s), len(s))
	quoteCmp := archsimd.BroadcastInt8x64('"')
	var result []byte
	lastWritten := 0
	skipNextQuote := false
	i := 0
	for i+simdChunkSize <= len(data) {
		chunk := archsimd.LoadInt8x64((*[simdChunkSize]int8)(unsafe.Pointer(&data[i])))
		mask := chunk.Equal(quoteCmp).ToBits()

		if skipNextQuote {
			if mask&1 != 0 {
				mask &^= 1
			}
			skipNextQuote = false
		}

		if mask != 0 {
			for mask != 0 {
				pos := bits.TrailingZeros64(mask)
				absPos := i + pos

				if absPos+1 < len(data) && data[absPos+1] == '"' {
					// Found double quote - write up to and including first quote
					if result == nil {
						result = make([]byte, 0, len(s))
					}
					result = append(result, s[lastWritten:absPos+1]...)
					lastWritten = absPos + 2 // Skip the second quote
					// Clear both bits if second quote is in same chunk
					mask &^= uint64(1) << pos
					if pos+1 < simdChunkSize {
						mask &^= uint64(1) << (pos + 1)
					} else {
						skipNextQuote = true
					}
					continue
				}
				mask &^= uint64(1) << pos
			}
		}
		i += simdChunkSize
	}

	// Process remaining bytes
	if skipNextQuote && i < len(data) && data[i] == '"' {
		i++
	}
	for i < len(data)-1 {
		if data[i] == '"' && data[i+1] == '"' {
			if result == nil {
				result = make([]byte, 0, len(s))
			}
			result = append(result, s[lastWritten:i+1]...)
			lastWritten = i + 2
			i += 2
			continue
		}
		i++
	}

	if result == nil {
		return s
	}
	if lastWritten < len(s) {
		result = append(result, s[lastWritten:]...)
	}
	return string(result)
}
