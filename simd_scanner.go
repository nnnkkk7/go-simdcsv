//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"simd/archsimd"
	"unsafe"

	"golang.org/x/sys/cpu"
)

// =============================================================================
// AVX-512 CPU Detection and Fallback
// =============================================================================
//
// NOTE: The simd/archsimd package in Go 1.26 is an experimental feature enabled via
// GOEXPERIMENT=simd. This package is AMD64-specific, and a higher-level portable
// SIMD package is planned for future development.
// See: https://github.com/golang/go/issues/73787 (archsimd proposal)
// See: https://go.dev/doc/go1.26 (Go 1.26 Release Notes)
//
// NOTE: The archsimd.Int8x32.Equal().ToBits() method internally uses the VPMOVB2M
// instruction (AVX-512BW). This instruction causes SIGILL (illegal instruction) on
// CPUs that do not support AVX-512, including GitHub Actions ubuntu-latest runners,
// most CI environments, and older CPUs.
//
// TODO: Revisit this fallback implementation when the simd/archsimd package provides:
//   - Mandatory runtime CPU feature checks within the archsimd package
//     (Issue #73787: "It is an open question whether we want to enforce that a CPU
//      feature check must be performed before using a vector intrinsic.")
//   - AVX2-only alternative to ToBits() (using VPMOVMSKB instruction)
//   - A high-level portable SIMD package
//
// TODO: Replace golang.org/x/sys/cpu usage with official archsimd API (e.g.,
// archsimd.HasAVX512()) when such API becomes available. Currently, the archsimd
// package does not provide CPU feature detection functions (as of Go 1.26).
//
// =============================================================================

// useAVX512 indicates whether AVX-512 instructions are available at runtime.
// This is set once at init time and used to dispatch to the appropriate implementation.
//
// NOTE: All three feature flags are required:
//   - AVX512F: Foundation 512-bit vector operations
//   - AVX512BW: Byte/word granularity operations (ToBits() uses VPMOVB2M)
//   - AVX512VL: 128/256-bit vector support with AVX-512 instructions
var useAVX512 bool

func init() {
	// NOTE: Using golang.org/x/sys/cpu for runtime CPU feature detection.
	// The archsimd package itself does not provide CPU detection functions (as of Go 1.26).
	useAVX512 = cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW && cpu.X86.HasAVX512VL
}

// scanState holds state carried between chunks during SIMD scanning
type scanState struct {
	quoted        uint64 // Quote state flag (0=outside, ^0=inside)
	skipNextQuote bool   // Skip quote at next chunk start (for boundary double quotes)
	prevEndedCR   bool   // Previous chunk ended with CR (for boundary CRLF)
}

// scanResult represents the result of SIMD scanning (bitmasks for structural characters)
type scanResult struct {
	quoteMasks     []uint64 // Quote masks per chunk
	separatorMasks []uint64 // Separator masks per chunk
	newlineMasks   []uint64 // Newline masks per chunk (CRLF normalized)
	postProcChunks []int    // Chunk indices containing double quotes
	finalQuoted    uint64   // Final quote state
	chunkCount     int      // Number of processed chunks
	lastChunkBits  int      // Valid bits in last chunk (if < 64)
}

// generateMasks generates 4 types of masks from a 64-byte chunk.
// It detects positions of quote ("), separator, carriage return (\r), and newline (\n).
// Precondition: data is at least 64 bytes.
//
// NOTE: This function dispatches to AVX-512 or scalar implementation based on the
// useAVX512 flag. In environments without AVX-512 support (CI, older CPUs), it falls
// back to the scalar implementation to avoid SIGILL.
func generateMasks(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	if useAVX512 {
		return generateMasksAVX512(data, separator)
	}
	return generateMasksScalar(data, separator)
}

// generateMasksAVX512 generates masks using AVX-512 SIMD instructions.
// Precondition: data is at least 64 bytes.
//
// NOTE: This implementation uses archsimd.Int8x32.Equal().ToBits().
// ToBits() internally generates the VPMOVB2M instruction (requires AVX-512BW),
// so this function cannot be executed on CPUs without AVX-512 support.
//
// TODO: If archsimd provides an AVX2-based alternative (using VPMOVMSKB instruction),
// this implementation could be updated to work with AVX2 as well.
// Currently, archsimd's ToBits() requires AVX-512.
func generateMasksAVX512(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	// Broadcast comparison values to all lanes of 256-bit vectors
	quoteCmp := archsimd.BroadcastInt8x32('"')
	sepCmp := archsimd.BroadcastInt8x32(int8(separator))
	crCmp := archsimd.BroadcastInt8x32('\r')
	nlCmp := archsimd.BroadcastInt8x32('\n')

	// Process low 32 bytes (positions 0-31)
	// Precondition: data is at least 64 bytes (guaranteed by caller)
	low := archsimd.LoadInt8x32((*[32]int8)(unsafe.Pointer(&data[0])))
	quoteLowMask := low.Equal(quoteCmp).ToBits()
	sepLowMask := low.Equal(sepCmp).ToBits()
	crLowMask := low.Equal(crCmp).ToBits()
	nlLowMask := low.Equal(nlCmp).ToBits()

	// Process high 32 bytes (positions 32-63)
	high := archsimd.LoadInt8x32((*[32]int8)(unsafe.Pointer(&data[32])))
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

// generateMasksScalar generates masks using scalar (non-SIMD) operations.
// This is the fallback implementation for CPUs without AVX-512 support.
// Precondition: data is at least 64 bytes.
func generateMasksScalar(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	for i := 0; i < 64; i++ {
		b := data[i]
		bit := uint64(1) << i
		if b == '"' {
			quote |= bit
		}
		if b == separator {
			sep |= bit
		}
		if b == '\r' {
			cr |= bit
		}
		if b == '\n' {
			nl |= bit
		}
	}
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
func processQuotesAndSeparators(quoteMask, sepMask, newlineMask, nextQuoteMask uint64, state *scanState) (quoteMaskOut, sepMaskOut uint64, hasDoubleQuote, boundaryDoubleQuote bool) {
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
			workSepMask &= ^(uint64(1) << sepPos)
		default:
			workNewlineMask &= ^(uint64(1) << nlPos)
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
		} else if quotePos < 63 && workQuoteMask&(uint64(1)<<(quotePos+1)) != 0 {
			// Adjacent double quote within chunk
			quoteMaskOut &= ^(uint64(3) << quotePos)
			hasDoubleQuote = true
			workQuoteMask &= ^(uint64(1) << (quotePos + 1))
		} else {
			quoted = 0 // Closing quote
		}
	} else {
		quoted = ^uint64(0) // Opening quote
	}
	workQuoteMask &= ^(uint64(1) << quotePos)
	return quoteMaskOut, hasDoubleQuote, boundaryDoubleQuote, quoted, workQuoteMask
}

// scanBuffer processes the entire buffer in 64-byte chunks using SIMD.
// It generates structural character masks and handles:
// - CRLF normalization (CRLF pairs are normalized to LF only in output)
// - Quote state tracking across chunk boundaries
// - Boundary double quote detection (quotes spanning chunks)
// - Recording chunks that need post-processing for double quote unescaping
func scanBuffer(buf []byte, separatorChar byte) *scanResult {
	if len(buf) == 0 {
		return &scanResult{}
	}

	chunkCount := (len(buf) + 63) / 64
	result := &scanResult{
		quoteMasks:     make([]uint64, 0, chunkCount),
		separatorMasks: make([]uint64, 0, chunkCount),
		newlineMasks:   make([]uint64, 0, chunkCount),
		postProcChunks: make([]int, 0, 32),
		chunkCount:     chunkCount,
	}

	state := scanState{}

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

		// Save the initial quoted state for newline invalidation
		initialQuoted := state.quoted

		// Process quotes and separators, invalidating those inside quoted regions
		quoteMaskOut, sepMaskOut, hasDoubleQuote, boundaryDoubleQuote := processQuotesAndSeparators(
			quoteMask, sepMask, newlineMaskOut, nextQuoteMask, &state,
		)

		// If there's a boundary double quote, the next chunk should skip its first quote
		if boundaryDoubleQuote {
			state.skipNextQuote = true
		}

		// Save end state and restore initial state for newline processing
		endQuoted := state.quoted
		state.quoted = initialQuoted

		// Invalidate newlines inside quoted regions using the processed quote mask
		// (with double quotes removed) and the initial state
		newlineMaskOut = invalidateNewlinesInQuotes(quoteMaskOut, newlineMaskOut, &state)

		// Restore end state for the next chunk
		state.quoted = endQuoted

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
func invalidateNewlinesInQuotes(quoteMask, newlineMask uint64, state *scanState) uint64 {
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
