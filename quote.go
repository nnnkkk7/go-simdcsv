//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"strings"
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
		absPos := chunkStart + pos

		if !isEscapedQuote(data, absPos) {
			return absPos, chunkStart, false
		}

		// Clear both bits of the escaped quote pair
		mask = clearBitU32(mask, pos)
		if pos+1 < simdHalfChunk {
			mask = clearBitU32(mask, pos+1)
		}

		// Handle boundary case: escaped quote spans chunk boundary
		if pos == simdHalfChunk-1 {
			newPos := chunkStart + simdHalfChunk
			if newPos < len(data) && data[newPos] == '"' {
				newPos++
			}
			return -1, newPos, false
		}
	}

	return -1, chunkStart + simdHalfChunk, false
}

// =============================================================================
// Content Extraction
// =============================================================================

// extractQuotedContent extracts the raw content from a quoted field.
// data should start from the opening quote.
// Returns the content between quotes without unescaping.
func extractQuotedContent(data []byte, closingQuoteIdx int) string {
	if closingQuoteIdx <= 1 {
		return ""
	}
	return string(data[1:closingQuoteIdx])
}

// =============================================================================
// Double Quote Unescaping - Unified Dispatch
// =============================================================================

// unescapeDoubleQuotes converts escaped double quotes ("") to single quotes (").
func unescapeDoubleQuotes(s string) string {
	if shouldUseSIMD(len(s)) {
		return unescapeDoubleQuotesSIMD(s)
	}
	return unescapeDoubleQuotesScalar(s)
}

// =============================================================================
// Double Quote Unescaping - Scalar Implementation
// =============================================================================

// unescapeDoubleQuotesScalar uses standard library for unescaping.
func unescapeDoubleQuotesScalar(s string) string {
	if !strings.Contains(s, `""`) {
		return s
	}
	return strings.ReplaceAll(s, `""`, `"`)
}

// =============================================================================
// Double Quote Unescaping - SIMD Implementation
// =============================================================================

// unescapeState holds state for SIMD double-quote unescaping.
type unescapeState struct {
	source        string
	data          []byte
	result        []byte
	lastWritten   int
	pos           int
	skipNextQuote bool
}

// unescapeDoubleQuotesSIMD uses AVX-512 to find and unescape double quotes.
func unescapeDoubleQuotesSIMD(s string) string {
	data := unsafe.Slice(unsafe.StringData(s), len(s))
	state := &unescapeState{source: s, data: data}

	state.processSIMDChunks()
	state.processRemainingBytes()

	return state.buildResult()
}

// processSIMDChunks processes full SIMD chunks looking for double quotes.
func (s *unescapeState) processSIMDChunks() {
	quoteCmp := archsimd.BroadcastInt8x64('"')

	for s.pos+simdChunkSize <= len(s.data) {
		chunk := archsimd.LoadInt8x64((*[simdChunkSize]int8)(unsafe.Pointer(&s.data[s.pos])))
		mask := chunk.Equal(quoteCmp).ToBits()

		mask = s.applySkipFlag(mask)
		s.processUnescapeMask(mask)
		s.pos += simdChunkSize
	}
}

// applySkipFlag clears the first bit if we need to skip due to boundary crossing.
func (s *unescapeState) applySkipFlag(mask uint64) uint64 {
	if s.skipNextQuote && mask&1 != 0 {
		mask &^= 1
	}
	s.skipNextQuote = false
	return mask
}

// processUnescapeMask processes all quote positions in a chunk mask for unescaping.
func (s *unescapeState) processUnescapeMask(mask uint64) {
	for mask != 0 {
		pos := bits.TrailingZeros64(mask)
		absPos := s.pos + pos

		if absPos+1 < len(s.data) && s.data[absPos+1] == '"' {
			s.recordUnescape(absPos)
			mask = clearBitU64(mask, pos)
			if pos+1 < simdChunkSize {
				mask = clearBitU64(mask, pos+1)
			} else {
				s.skipNextQuote = true
			}
			continue
		}
		mask = clearBitU64(mask, pos)
	}
}

// recordUnescape records a double-quote unescape at the given position.
func (s *unescapeState) recordUnescape(absPos int) {
	if s.result == nil {
		s.result = make([]byte, 0, len(s.source))
	}
	s.result = append(s.result, s.source[s.lastWritten:absPos+1]...)
	s.lastWritten = absPos + 2
}

// processRemainingBytes handles bytes after the last full SIMD chunk.
func (s *unescapeState) processRemainingBytes() {
	if s.skipNextQuote && s.pos < len(s.data) && s.data[s.pos] == '"' {
		s.pos++
	}

	for s.pos < len(s.data)-1 {
		if s.data[s.pos] == '"' && s.data[s.pos+1] == '"' {
			s.recordUnescape(s.pos)
			s.pos += 2
			continue
		}
		s.pos++
	}
}

// buildResult returns the final unescaped string.
func (s *unescapeState) buildResult() string {
	if s.result == nil {
		return s.source
	}
	if s.lastWritten < len(s.source) {
		s.result = append(s.result, s.source[s.lastWritten:]...)
	}
	return string(s.result)
}

// =============================================================================
// Bit Manipulation Utilities
// =============================================================================

// clearBitU32 clears the bit at position pos in a 32-bit mask.
func clearBitU32(mask uint32, pos int) uint32 {
	return mask &^ (uint32(1) << pos)
}

// clearBitU64 clears the bit at position pos in a 64-bit mask.
func clearBitU64(mask uint64, pos int) uint64 {
	return mask &^ (uint64(1) << pos)
}
