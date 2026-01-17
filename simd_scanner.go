//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"simd/archsimd"
	"unsafe"
)

// stage1State holds state carried between chunks
type stage1State struct {
	quoted        uint64 // Quote state flag (0=outside, ^0=inside)
	skipNextQuote bool   // Skip quote at next chunk start (for boundary double quotes)
	prevEndedCR   bool   // Previous chunk ended with CR (for boundary CRLF)
}

// stage1Result represents Stage 1 processing result
type stage1Result struct {
	quoteMasks     []uint64 // Quote masks per chunk
	separatorMasks []uint64 // Separator masks per chunk
	newlineMasks   []uint64 // Newline masks per chunk (CRLF normalized)
	postProcChunks []int    // Chunk indices containing double quotes
	finalQuoted    uint64   // Final quote state
	chunkCount     int      // Number of processed chunks
	lastChunkBits  int      // Valid bits in last chunk (if < 64)
}

// generateMasks generates 4 types of masks from a 64-byte chunk using SIMD.
// It detects positions of quote ("), separator, carriage return (\r), and newline (\n).
// Precondition: data is at least 64 bytes.
func generateMasks(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	// Broadcast comparison values to all lanes of 256-bit vectors
	quoteCmp := archsimd.BroadcastInt8x32('"')
	sepCmp := archsimd.BroadcastInt8x32(int8(separator))
	crCmp := archsimd.BroadcastInt8x32('\r')
	nlCmp := archsimd.BroadcastInt8x32('\n')

	// Process low 32 bytes (positions 0-31)
	// Precondition: data is at least 64 bytes (guaranteed by caller)
	low := archsimd.LoadInt8x32((*[32]int8)(unsafe.Pointer(&data[0]))) //nolint:gosec // G602: bounds checked by precondition
	quoteLowMask := low.Equal(quoteCmp).ToBits()
	sepLowMask := low.Equal(sepCmp).ToBits()
	crLowMask := low.Equal(crCmp).ToBits()
	nlLowMask := low.Equal(nlCmp).ToBits()

	// Process high 32 bytes (positions 32-63)
	high := archsimd.LoadInt8x32((*[32]int8)(unsafe.Pointer(&data[32]))) //nolint:gosec // G602: bounds checked by precondition
	quoteHighMask := high.Equal(quoteCmp).ToBits()
	sepHighMask := high.Equal(sepCmp).ToBits()
	crHighMask := high.Equal(crCmp).ToBits()
	nlHighMask := high.Equal(nlCmp).ToBits()

	// Combine into 64-bit masks (low bits 0-31, high bits 32-63)
	quote = uint64(quoteLowMask) | (uint64(quoteHighMask) << 32)
	sep = uint64(sepLowMask) | (uint64(sepHighMask) << 32)
	cr = uint64(crLowMask) | (uint64(crHighMask) << 32)
	nl = uint64(nlLowMask) | (uint64(nlHighMask) << 32)

	return
}

// generateMasksPadded processes chunks smaller than 64 bytes.
// It copies data to a 64-byte buffer (stack allocated), generates masks,
// then masks off invalid bits beyond the actual data length.
// Returns masks only for valid bytes (remaining bits are 0).
func generateMasksPadded(data []byte, separator byte) (quote, sep, cr, nl uint64, validBits int) {
	validBits = len(data)
	if validBits == 0 {
		return 0, 0, 0, 0, 0
	}

	// Create a 64-byte padded buffer on the stack
	var padded [64]byte
	copy(padded[:], data)
	// Remaining bytes are zero (won't match any structural characters)

	// Generate masks from the padded buffer
	quote, sep, cr, nl = generateMasks(padded[:], separator)

	// Mask off bits beyond valid data
	if validBits < 64 {
		mask := (uint64(1) << validBits) - 1
		quote &= mask
		sep &= mask
		cr &= mask
		nl &= mask
	}

	return
}

// processQuotesAndSeparators processes quote and separator masks to handle:
// - Quote state tracking (inside/outside quoted regions)
// - Invalidating separators and newlines inside quotes
// - Detecting double quotes ("") for escaping
// - Detecting boundary double quotes (quote at position 63 with quote at position 0 of next chunk)
//
// Returns:
// - quoteMaskOut: adjusted quote mask with escaped double quotes removed
// - sepMaskOut: separator mask with separators inside quotes removed
// - hasDoubleQuote: true if this chunk contains escaped double quotes (needs post-processing)
// - boundaryDoubleQuote: true if there's a double quote spanning chunk boundary
func processQuotesAndSeparators(quoteMask, sepMask, newlineMask, nextQuoteMask uint64, state *stage1State) (quoteMaskOut, sepMaskOut uint64, hasDoubleQuote, boundaryDoubleQuote bool) {
	quoteMaskOut = quoteMask
	sepMaskOut = sepMask

	workQuoteMask := quoteMask
	workSepMask := sepMask
	workNewlineMask := newlineMask
	quoted := state.quoted

	for {
		quotePos := bits.TrailingZeros64(workQuoteMask)
		sepPos := bits.TrailingZeros64(workSepMask)
		nlPos := bits.TrailingZeros64(workNewlineMask)

		minPos := minOfThree(quotePos, sepPos, nlPos)
		if minPos >= 64 {
			break
		}

		switch minPos {
		case quotePos:
			quoteMaskOut, hasDoubleQuote, boundaryDoubleQuote, quoted, workQuoteMask = processQuote(
				quotePos, quoted, workQuoteMask, quoteMaskOut, nextQuoteMask, hasDoubleQuote, boundaryDoubleQuote,
			)
		case sepPos:
			if quoted != 0 {
				sepMaskOut &= ^(uint64(1) << sepPos)
			}
			workSepMask &= ^uint64(1) << sepPos
		default:
			workNewlineMask &= ^uint64(1) << nlPos
		}
	}

	state.quoted = quoted
	return quoteMaskOut, sepMaskOut, hasDoubleQuote, boundaryDoubleQuote
}

// minOfThree returns the minimum of three integers.
func minOfThree(a, b, c int) int {
	if a <= b && a <= c {
		return a
	}
	if b <= c {
		return b
	}
	return c
}

// processQuote handles quote character processing and returns updated state.
func processQuote(quotePos int, quoted, workQuoteMask, quoteMaskOut, nextQuoteMask uint64, hasDoubleQuote, boundaryDoubleQuote bool) (uint64, bool, bool, uint64, uint64) {
	if quoted != 0 {
		// Inside quoted region - check for escape sequences
		if quotePos == 63 && nextQuoteMask&1 != 0 {
			// Boundary double quote
			quoteMaskOut &= ^(uint64(1) << 63)
			hasDoubleQuote = true
			boundaryDoubleQuote = true
		} else if quotePos < 63 && workQuoteMask&(1<<(quotePos+1)) != 0 {
			// Adjacent double quote within chunk
			quoteMaskOut &= ^(uint64(3) << quotePos)
			hasDoubleQuote = true
			workQuoteMask &= ^uint64(1) << (quotePos + 1)
		} else {
			quoted = 0 // Closing quote
		}
	} else {
		quoted = ^uint64(0) // Opening quote
	}
	workQuoteMask &= ^uint64(1) << quotePos
	return quoteMaskOut, hasDoubleQuote, boundaryDoubleQuote, quoted, workQuoteMask
}

// stage1PreprocessBuffer processes the entire buffer in 64-byte chunks.
// It generates structural character masks and handles:
// - CRLF normalization (CRLF pairs are normalized to LF only in output)
// - Quote state tracking across chunk boundaries
// - Boundary double quote detection (quotes spanning chunks)
// - Recording chunks that need post-processing for double quote unescaping
func stage1PreprocessBuffer(buf []byte, separatorChar byte) *stage1Result {
	if len(buf) == 0 {
		return &stage1Result{}
	}

	chunkCount := (len(buf) + 63) / 64
	result := &stage1Result{
		quoteMasks:     make([]uint64, 0, chunkCount),
		separatorMasks: make([]uint64, 0, chunkCount),
		newlineMasks:   make([]uint64, 0, chunkCount),
		postProcChunks: make([]int, 0, 32),
		chunkCount:     chunkCount,
	}

	state := stage1State{}

	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		offset := chunkIdx * 64
		remaining := len(buf) - offset

		// Generate masks (last chunk may need padding)
		var quoteMask, sepMask, crMask, nlMask uint64
		var validBits int
		if remaining >= 64 {
			quoteMask, sepMask, crMask, nlMask = generateMasks(buf[offset:offset+64], separatorChar)
			validBits = 64
		} else {
			quoteMask, sepMask, crMask, nlMask, validBits = generateMasksPadded(buf[offset:], separatorChar)
			result.lastChunkBits = validBits
		}

		// Lookahead to next chunk for boundary handling
		var nextQuoteMask, nextNlMask uint64
		if remaining > 64 {
			nextRemaining := remaining - 64
			if nextRemaining >= 64 {
				nextQuoteMask, _, _, nextNlMask = generateMasks(buf[offset+64:offset+128], separatorChar)
			} else {
				nextQuoteMask, _, _, nextNlMask, _ = generateMasksPadded(buf[offset+64:], separatorChar)
			}
		}

		// Handle boundary double quote from previous chunk
		// If previous chunk ended with a quote that's part of a double quote sequence,
		// skip the first quote of this chunk
		if state.skipNextQuote && quoteMask&1 != 0 {
			quoteMask &= ^uint64(1) // Skip the first quote
		}
		state.skipNextQuote = false

		// Handle boundary CRLF from previous chunk
		// If previous chunk ended with CR and this chunk starts with LF,
		// the CR was already excluded from previous chunk's newline mask
		// and we keep this LF as the record delimiter (no action needed)
		state.prevEndedCR = false

		// CRLF normalization:
		// For CRLF pairs within this chunk, we want only the LF to appear in newlineMask
		// CR followed by LF at positions i and i+1 should result in only bit i+1 set
		newlineMaskOut := nlMask

		// Find CRLF pairs: CR at position i, LF at position i+1
		// crMask & (nlMask >> 1) gives us CRs that are followed by LF
		crlfPairs := crMask & (nlMask >> 1)

		// Isolated CRs (CR not followed by LF) should be treated as newlines
		isolatedCRs := crMask & ^crlfPairs
		newlineMaskOut |= isolatedCRs

		// Handle CR at position 63 (may be part of boundary CRLF)
		if validBits == 64 && crMask&(1<<63) != 0 {
			if nextNlMask&1 != 0 {
				// Boundary CRLF: CR at 63, LF at next chunk's 0
				// Remove this CR from newline mask (next chunk's LF will be the delimiter)
				newlineMaskOut &= ^(uint64(1) << 63)
				state.prevEndedCR = true
			} else {
				// Isolated CR at position 63: treat as newline
				newlineMaskOut |= uint64(1) << 63
			}
		}

		// Process quotes and separators, invalidating those inside quoted regions
		quoteMaskOut, sepMaskOut, hasDoubleQuote, boundaryDoubleQuote := processQuotesAndSeparators(
			quoteMask, sepMask, newlineMaskOut, nextQuoteMask, &state,
		)

		// If there's a boundary double quote, the next chunk should skip its first quote
		if boundaryDoubleQuote {
			state.skipNextQuote = true
		}

		// Also need to invalidate newlines inside quoted regions
		// Re-process to get the final newline mask
		newlineMaskOut = invalidateNewlinesInQuotes(quoteMask, newlineMaskOut, &state)

		// Store results
		result.quoteMasks = append(result.quoteMasks, quoteMaskOut)
		result.separatorMasks = append(result.separatorMasks, sepMaskOut)
		result.newlineMasks = append(result.newlineMasks, newlineMaskOut)

		// Record chunks that need post-processing for double quote unescaping
		if hasDoubleQuote {
			result.postProcChunks = append(result.postProcChunks, chunkIdx)
		}
	}

	result.finalQuoted = state.quoted

	return result
}

// invalidateNewlinesInQuotes removes newline bits that are inside quoted regions.
func invalidateNewlinesInQuotes(quoteMask, newlineMask uint64, state *stage1State) uint64 {
	quoted := state.quoted
	result := newlineMask
	workQuoteMask := quoteMask
	workNewlineMask := newlineMask

	for workQuoteMask != 0 || workNewlineMask != 0 {
		quotePos := bits.TrailingZeros64(workQuoteMask)
		nlPos := bits.TrailingZeros64(workNewlineMask)

		if quotePos >= 64 && nlPos >= 64 {
			break
		}

		if quotePos < nlPos {
			// Toggle quote state
			if quoted != 0 {
				quoted = 0
			} else {
				quoted = ^uint64(0)
			}
			workQuoteMask &= ^(uint64(1) << quotePos)
		} else {
			if quoted != 0 {
				result &= ^(uint64(1) << nlPos)
			}
			workNewlineMask &= ^(uint64(1) << nlPos)
		}
	}

	return result
}
