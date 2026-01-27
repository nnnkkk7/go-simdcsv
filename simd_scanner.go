//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"simd/archsimd"
	"sync"
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

// SIMD processing constants
const (
	// simdChunkSize is the number of bytes processed per SIMD iteration (AVX-512 = 64 bytes).
	simdChunkSize = 64

	// simdHalfChunk is the size of a half SIMD chunk (AVX2/half AVX-512 = 32 bytes).
	simdHalfChunk = 32

	// simdMinThreshold is the minimum data size for SIMD optimization to be beneficial.
	simdMinThreshold = 32

	// avgFieldLenEstimate is the estimated average field length for capacity pre-allocation.
	avgFieldLenEstimate = 15

	// avgRowLenEstimate is the estimated average row length for capacity pre-allocation.
	avgRowLenEstimate = 80
)

func init() {
	// NOTE: Using golang.org/x/sys/cpu for runtime CPU feature detection.
	// The archsimd package itself does not provide CPU detection functions (as of Go 1.26).
	useAVX512 = cpu.X86.HasAVX512F && cpu.X86.HasAVX512BW && cpu.X86.HasAVX512VL
}

// =============================================================================
// SIMD/Scalar Dispatch Utilities
// =============================================================================

// shouldUseSIMD returns true if SIMD should be used for the given data length.
// This centralizes the SIMD eligibility check used across multiple functions.
func shouldUseSIMD(dataLen int) bool {
	return useAVX512 && dataLen >= simdMinThreshold
}

// scanState holds state carried between chunks during SIMD scanning
type scanState struct {
	quoted        uint64 // Quote state flag (0=outside, ^0=inside)
	skipNextQuote bool   // Skip quote at next chunk start (for boundary double quotes)
}

// scanResult represents the result of SIMD scanning (bitmasks for structural characters)
type scanResult struct {
	quoteMasks     []uint64 // Quote masks per chunk
	separatorMasks []uint64 // Separator masks per chunk
	newlineMasks   []uint64 // Newline masks per chunk (CRLF normalized)
	chunkHasDQ     []bool   // Per-chunk flag: true if chunk contains escaped double quotes
	hasQuotes      bool     // True if any quote characters exist in input
	finalQuoted    uint64   // Final quote state
	chunkCount     int      // Number of processed chunks
	lastChunkBits  int      // Valid bits in last chunk (if < 64)
}

// scanResultPoolCapacity is the pre-allocated slice capacity for pooled scanResult objects.
// 1024 chunks = ~64KB input (1024 * 64 bytes per chunk).
const scanResultPoolCapacity = 1024

// scanResultPool provides reusable scanResult objects to reduce allocations.
var scanResultPool = sync.Pool{
	New: func() interface{} {
		return &scanResult{
			quoteMasks:     make([]uint64, 0, scanResultPoolCapacity),
			separatorMasks: make([]uint64, 0, scanResultPoolCapacity),
			newlineMasks:   make([]uint64, 0, scanResultPoolCapacity),
			chunkHasDQ:     make([]bool, 0, scanResultPoolCapacity),
		}
	},
}

// reset clears the scanResult for reuse while preserving underlying slice capacity.
func (sr *scanResult) reset() {
	sr.quoteMasks = sr.quoteMasks[:0]
	sr.separatorMasks = sr.separatorMasks[:0]
	sr.newlineMasks = sr.newlineMasks[:0]
	if cap(sr.chunkHasDQ) > 0 {
		sr.chunkHasDQ = sr.chunkHasDQ[:0]
	}
	sr.hasQuotes = false
	sr.finalQuoted = 0
	sr.chunkCount = 0
	sr.lastChunkBits = 0
}

// releaseScanResult returns a scanResult to the pool for reuse.
// The caller must not use the scanResult after calling this function.
func releaseScanResult(sr *scanResult) {
	if sr != nil {
		sr.reset()
		scanResultPool.Put(sr)
	}
}

// generateMasks generates 4 types of masks from a simdChunkSize-byte chunk.
// It detects positions of quote ("), separator, carriage return (\r), and newline (\n).
// Precondition: data is at least simdChunkSize bytes.
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
// Precondition: data is at least simdChunkSize bytes.
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

	// Process low simdHalfChunk bytes (positions 0-31)
	// Precondition: data is at least simdChunkSize bytes (guaranteed by caller)
	low := archsimd.LoadInt8x32((*[simdHalfChunk]int8)(unsafe.Pointer(&data[0])))
	quoteLowMask := low.Equal(quoteCmp).ToBits()
	sepLowMask := low.Equal(sepCmp).ToBits()
	crLowMask := low.Equal(crCmp).ToBits()
	nlLowMask := low.Equal(nlCmp).ToBits()

	// Process high simdHalfChunk bytes (positions 32-63)
	high := archsimd.LoadInt8x32((*[simdHalfChunk]int8)(unsafe.Pointer(&data[simdHalfChunk])))
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
// Precondition: data is at least simdChunkSize bytes.
func generateMasksScalar(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	for i := 0; i < simdChunkSize; i++ {
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

// generateMasksPadded processes chunks smaller than simdChunkSize bytes.
// It copies data to a simdChunkSize-byte buffer (stack allocated), generates masks,
// then masks off invalid bits beyond the actual data length.
// Returns masks only for valid bytes (remaining bits are 0).
func generateMasksPadded(data []byte, separator byte) (quote, sep, cr, nl uint64, validBits int) {
	validBits = len(data)
	if validBits == 0 {
		return 0, 0, 0, 0, 0
	}

	// Create a simdChunkSize-byte padded buffer on the stack
	var padded [simdChunkSize]byte
	copy(padded[:], data)
	// Remaining bytes are zero (won't match any structural characters)

	// Generate masks from the padded buffer
	quote, sep, cr, nl = generateMasks(padded[:], separator)

	// Mask off bits beyond valid data
	if validBits < simdChunkSize {
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
		if minPos >= simdChunkSize {
			break
		}

		switch minPos {
		case quotePos:
			// Process quote character (inlined for simplicity)
			if quoted != 0 {
				// Inside quoted region - check for escape sequences
				if quotePos == simdChunkSize-1 && nextQuoteMask&1 != 0 {
					// Boundary double quote
					quoteMaskOut &= ^(uint64(1) << (simdChunkSize - 1))
					hasDoubleQuote = true
					boundaryDoubleQuote = true
				} else if quotePos < simdChunkSize-1 && workQuoteMask&(uint64(1)<<(quotePos+1)) != 0 {
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

// chunkMasks holds the four mask types for a single chunk
type chunkMasks struct {
	quote uint64
	sep   uint64
	cr    uint64
	nl    uint64
}

// scanBuffer processes the entire buffer in simdChunkSize-byte chunks using SIMD.
// It generates structural character masks and handles:
// - CRLF normalization (CRLF pairs are normalized to LF only in output)
// - Quote state tracking across chunk boundaries
// - Boundary double quote detection (quotes spanning chunks)
// - Recording chunks that need post-processing for double quote unescaping
func scanBuffer(buf []byte, separatorChar byte) *scanResult {
	if len(buf) == 0 {
		return &scanResult{}
	}

	chunkCount := (len(buf) + simdChunkSize - 1) / simdChunkSize

	// Get a scanResult from the pool and reset it for reuse
	result := scanResultPool.Get().(*scanResult)
	result.reset()
	result.chunkCount = chunkCount

	// Pre-size all mask slices to chunkCount for index-based assignment (avoids append overhead)
	// When capacity is insufficient, grow by 2x to reduce future reallocations
	if cap(result.quoteMasks) < chunkCount {
		newCap := chunkCount
		if newCap < cap(result.quoteMasks)*2 {
			newCap = cap(result.quoteMasks) * 2
		}
		result.quoteMasks = make([]uint64, chunkCount, newCap)
	} else {
		result.quoteMasks = result.quoteMasks[:chunkCount]
	}
	if cap(result.separatorMasks) < chunkCount {
		newCap := chunkCount
		if newCap < cap(result.separatorMasks)*2 {
			newCap = cap(result.separatorMasks) * 2
		}
		result.separatorMasks = make([]uint64, chunkCount, newCap)
	} else {
		result.separatorMasks = result.separatorMasks[:chunkCount]
	}
	if cap(result.newlineMasks) < chunkCount {
		newCap := chunkCount
		if newCap < cap(result.newlineMasks)*2 {
			newCap = cap(result.newlineMasks) * 2
		}
		result.newlineMasks = make([]uint64, chunkCount, newCap)
	} else {
		result.newlineMasks = result.newlineMasks[:chunkCount]
	}
	if cap(result.chunkHasDQ) < chunkCount {
		newCap := chunkCount
		if newCap < cap(result.chunkHasDQ)*2 {
			newCap = cap(result.chunkHasDQ) * 2
		}
		result.chunkHasDQ = make([]bool, chunkCount, newCap)
	} else {
		result.chunkHasDQ = result.chunkHasDQ[:chunkCount]
		// Clear the slice (reset only truncates, doesn't zero)
		for i := range result.chunkHasDQ {
			result.chunkHasDQ[i] = false
		}
	}

	state := scanState{}

	// Pre-compute masks for chunk 0 (current) and chunk 1 (next) to avoid double calculation
	var curMasks, nextMasks chunkMasks
	var curValidBits int

	// Generate masks for chunk 0
	if len(buf) >= simdChunkSize {
		curMasks.quote, curMasks.sep, curMasks.cr, curMasks.nl = generateMasks(buf[0:simdChunkSize], separatorChar)
		curValidBits = simdChunkSize
	} else {
		curMasks.quote, curMasks.sep, curMasks.cr, curMasks.nl, curValidBits = generateMasksPadded(buf, separatorChar)
		result.lastChunkBits = curValidBits
	}

	// Generate masks for chunk 1 (lookahead) if it exists
	// Note: chunkCount > 1 implies len(buf) > simdChunkSize, so buf[simdChunkSize:] is safe
	if chunkCount > 1 && len(buf) > simdChunkSize {
		if len(buf) >= 2*simdChunkSize {
			nextMasks.quote, nextMasks.sep, nextMasks.cr, nextMasks.nl = generateMasks(buf[simdChunkSize:2*simdChunkSize], separatorChar)
		} else {
			var nextValidBits int
			nextMasks.quote, nextMasks.sep, nextMasks.cr, nextMasks.nl, nextValidBits = generateMasksPadded(buf[simdChunkSize:], separatorChar)
			// If chunk 1 is the last chunk, set lastChunkBits
			if chunkCount == 2 {
				result.lastChunkBits = nextValidBits
			}
		}
	}

	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		// Use pre-computed masks
		quoteMask := curMasks.quote
		sepMask := curMasks.sep
		crMask := curMasks.cr
		nlMask := curMasks.nl
		validBits := curValidBits

		// Lookahead masks are already in nextMasks
		nextQuoteMask := nextMasks.quote
		nextNlMask := nextMasks.nl

		// Handle boundary double quote from previous chunk
		// If previous chunk ended with a quote that's part of a double quote sequence,
		// skip the first quote of this chunk
		if state.skipNextQuote && quoteMask&1 != 0 {
			quoteMask &= ^uint64(1) // Skip the first quote
		}
		state.skipNextQuote = false

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
		if validBits == simdChunkSize && crMask&(1<<63) != 0 {
			if nextNlMask&1 != 0 {
				// Boundary CRLF: CR at 63, LF at next chunk's 0
				// Remove this CR from newline mask (next chunk's LF will be the delimiter)
				newlineMaskOut &= ^(uint64(1) << 63)
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

		// Store results using index assignment (pre-sized slices)
		result.quoteMasks[chunkIdx] = quoteMaskOut
		result.separatorMasks[chunkIdx] = sepMaskOut
		result.newlineMasks[chunkIdx] = newlineMaskOut

		// Track if any quotes exist in the input (for fast path optimization)
		if quoteMaskOut != 0 {
			result.hasQuotes = true
		}

		// Record chunks that have double quotes (using bool array instead of []int)
		if hasDoubleQuote {
			result.chunkHasDQ[chunkIdx] = true
		}

		// Slide masks: current = next, compute new next for chunkIdx+2
		curMasks = nextMasks
		curValidBits = simdChunkSize // next chunk was full unless it's the last

		nextChunkIdx := chunkIdx + 2
		if nextChunkIdx < chunkCount {
			nextOffset := nextChunkIdx * simdChunkSize
			remaining := len(buf) - nextOffset
			if remaining >= simdChunkSize {
				nextMasks.quote, nextMasks.sep, nextMasks.cr, nextMasks.nl = generateMasks(buf[nextOffset:nextOffset+simdChunkSize], separatorChar)
			} else {
				nextMasks.quote, nextMasks.sep, nextMasks.cr, nextMasks.nl, curValidBits = generateMasksPadded(buf[nextOffset:], separatorChar)
				result.lastChunkBits = curValidBits
				// curValidBits is for the NEXT iteration's current chunk
			}
		} else {
			// No more chunks after next iteration
			nextMasks = chunkMasks{}
			// Check if next iteration is the last chunk and needs partial bits
			if chunkIdx+1 == chunkCount-1 && len(buf)%simdChunkSize != 0 {
				curValidBits = len(buf) % simdChunkSize
				result.lastChunkBits = curValidBits
			}
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
