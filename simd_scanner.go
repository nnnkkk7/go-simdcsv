//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"simd/archsimd"
	"sync"
	"unsafe"
)

// useAVX512 indicates whether AVX-512 instructions are available at runtime.
var useAVX512 bool

// SIMD processing constants.
const (
	simdChunkSize       = 64 // bytes per AVX-512 iteration
	simdHalfChunk       = 32 // bytes for half chunk (AVX2 size)
	simdMinThreshold    = 32 // minimum size for SIMD benefit
	avgFieldLenEstimate = 15 // estimated avg field length for preallocation
	avgRowLenEstimate   = 80 // estimated avg row length for preallocation
)

func init() {
	useAVX512 = archsimd.X86.AVX512()
}

// =============================================================================
// SIMD/Scalar Dispatch Utilities
// =============================================================================

// shouldUseSIMD returns true if SIMD should be used for the given data length.
// This centralizes the SIMD eligibility check used across multiple functions.
func shouldUseSIMD(dataLen int) bool {
	return useAVX512 && dataLen >= simdMinThreshold
}

// =============================================================================
// Core Data Structures
// =============================================================================

// scanState holds state carried between chunks during scanning.
type scanState struct {
	quoted        uint64 // 0 = outside quotes, ^0 = inside quotes
	skipNextQuote bool   // skip first quote of next chunk (boundary double quote)
}

// scanResult holds bitmasks for structural characters from scanning.
type scanResult struct {
	quoteMasks     []uint64 // quote positions per chunk
	separatorMasks []uint64 // separator positions per chunk
	newlineMasks   []uint64 // newline positions per chunk (CRLF normalized)
	chunkHasDQ     []bool   // chunks containing escaped double quotes
	chunkHasQuote  []bool   // chunks containing any quote
	hasQuotes      bool     // input contains quote characters
	hasCR          bool     // input contains carriage returns
	finalQuoted    uint64   // quote state after scanning
	chunkCount     int      // number of chunks processed
	lastChunkBits  int      // valid bits in final chunk (< 64)
	separatorCount int      // total separators (outside quotes)
	newlineCount   int      // total newlines (outside quotes)
}

// chunkMasks holds the four mask types for a single 64-byte chunk.
type chunkMasks struct {
	quote, sep, cr, nl uint64
}

// =============================================================================
// Object Pooling
// =============================================================================

// scanResultPoolCapacity is the pre-allocated capacity for pooled scanResult slices.
// 512 chunks = ~32KB input, balancing small and large file performance.
const scanResultPoolCapacity = 512

// scanResultPool provides reusable scanResult objects to reduce allocations.
var scanResultPool = sync.Pool{
	New: func() interface{} {
		return &scanResult{
			quoteMasks:     make([]uint64, 0, scanResultPoolCapacity),
			separatorMasks: make([]uint64, 0, scanResultPoolCapacity),
			newlineMasks:   make([]uint64, 0, scanResultPoolCapacity),
			chunkHasDQ:     make([]bool, 0, scanResultPoolCapacity),
			chunkHasQuote:  make([]bool, 0, scanResultPoolCapacity),
		}
	},
}

// reset clears the scanResult for reuse while preserving slice capacity.
func (sr *scanResult) reset() {
	sr.quoteMasks = sr.quoteMasks[:0]
	sr.separatorMasks = sr.separatorMasks[:0]
	sr.newlineMasks = sr.newlineMasks[:0]
	sr.chunkHasDQ = sr.chunkHasDQ[:0]
	sr.chunkHasQuote = sr.chunkHasQuote[:0]
	sr.hasQuotes = false
	sr.hasCR = false
	sr.finalQuoted = 0
	sr.chunkCount = 0
	sr.lastChunkBits = 0
	sr.separatorCount = 0
	sr.newlineCount = 0
}

// releaseScanResult returns a scanResult to the pool for reuse.
func releaseScanResult(sr *scanResult) {
	if sr != nil {
		sr.reset()
		scanResultPool.Put(sr)
	}
}

// =============================================================================
// Slice Utilities
// =============================================================================

// ensureUint64SliceCap ensures slice has at least required length.
// Uses 2x growth with 25% headroom when reallocation is needed.
func ensureUint64SliceCap(s []uint64, required int) []uint64 {
	if cap(s) >= required {
		return s[:required]
	}
	newCap := max(cap(s)*2, required)
	newCap += newCap / 4
	return make([]uint64, required, newCap)
}

// ensureBoolSliceCap ensures slice has at least required length (cleared).
// Uses 2x growth with 25% headroom when reallocation is needed.
func ensureBoolSliceCap(s []bool, required int) []bool {
	if cap(s) >= required {
		s = s[:required]
		clear(s)
		return s
	}
	newCap := max(cap(s)*2, required)
	newCap += newCap / 4
	return make([]bool, required, newCap)
}

// =============================================================================
// Mask Generation - Scalar Implementation
// =============================================================================

// generateMasksScalar generates masks using scalar operations (fallback for non-AVX-512).
func generateMasksScalar(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	for i := 0; i < simdChunkSize; i++ {
		bit := uint64(1) << i
		switch data[i] {
		case '"':
			quote |= bit
		case separator:
			sep |= bit
		case '\r':
			cr |= bit
		case '\n':
			nl |= bit
		}
	}
	return
}

// =============================================================================
// Mask Generation - AVX-512 Implementation
// =============================================================================

// generateMasksAVX512 generates masks using AVX-512 SIMD instructions.
// Requires AVX-512BW for ToBits() which uses VPMOVB2M instruction.
func generateMasksAVX512(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	quoteCmp := archsimd.BroadcastInt8x64('"')
	sepCmp := archsimd.BroadcastInt8x64(int8(separator))
	crCmp := archsimd.BroadcastInt8x64('\r')
	nlCmp := archsimd.BroadcastInt8x64('\n')
	return generateMasksAVX512WithCmp(data, quoteCmp, sepCmp, crCmp, nlCmp)
}

// generateMasksAVX512WithCmp generates masks reusing pre-broadcasted comparators.
func generateMasksAVX512WithCmp(data []byte, quoteCmp, sepCmp, crCmp, nlCmp archsimd.Int8x64) (quote, sep, cr, nl uint64) {
	chunk := archsimd.LoadInt8x64((*[simdChunkSize]int8)(unsafe.Pointer(&data[0])))
	return chunk.Equal(quoteCmp).ToBits(),
		chunk.Equal(sepCmp).ToBits(),
		chunk.Equal(crCmp).ToBits(),
		chunk.Equal(nlCmp).ToBits()
}

// =============================================================================
// Mask Generation - Unified Dispatch
// =============================================================================

// generateMasks generates bitmasks for structural characters in a 64-byte chunk.
// Returns masks for quote, separator, CR, and newline positions.
// Dispatches to AVX-512 or scalar implementation based on CPU support.
func generateMasks(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	if useAVX512 {
		return generateMasksAVX512(data, separator)
	}
	return generateMasksScalar(data, separator)
}

// generateMasksPadded processes chunks smaller than 64 bytes by zero-padding.
// Returns masks with bits beyond valid data cleared.
func generateMasksPadded(data []byte, separator byte) (quote, sep, cr, nl uint64, validBits int) {
	validBits = len(data)
	if validBits == 0 {
		return 0, 0, 0, 0, 0
	}

	var padded [simdChunkSize]byte
	copy(padded[:], data)

	quote, sep, cr, nl = generateMasks(padded[:], separator)

	if validBits < simdChunkSize {
		mask := (uint64(1) << validBits) - 1
		quote &= mask
		sep &= mask
		cr &= mask
		nl &= mask
	}
	return
}

// generateMasksPaddedWithCmp is the AVX-512 version of generateMasksPadded.
func generateMasksPaddedWithCmp(data []byte, quoteCmp, sepCmp, crCmp, nlCmp archsimd.Int8x64) (quote, sep, cr, nl uint64, validBits int) {
	validBits = len(data)
	if validBits == 0 {
		return 0, 0, 0, 0, 0
	}

	var padded [simdChunkSize]byte
	copy(padded[:], data)

	quote, sep, cr, nl = generateMasksAVX512WithCmp(padded[:], quoteCmp, sepCmp, crCmp, nlCmp)

	if validBits < simdChunkSize {
		mask := (uint64(1) << validBits) - 1
		quote &= mask
		sep &= mask
		cr &= mask
		nl &= mask
	}
	return
}

// =============================================================================
// CRLF Normalization
// =============================================================================

// normalizeCRLF converts CRLF pairs to LF and handles isolated CRs.
// Returns the normalized newline mask.
func normalizeCRLF(crMask, nlMask, nextNlMask uint64, validBits int) uint64 {
	newlineMaskOut := nlMask

	// CRLF pairs: CR followed by LF
	crlfPairs := crMask & (nlMask >> 1)
	// Isolated CRs should be treated as newlines
	isolatedCRs := crMask & ^crlfPairs
	newlineMaskOut |= isolatedCRs

	// Handle boundary CRLF (CR at position 63)
	if validBits == simdChunkSize && crMask&(1<<63) != 0 {
		if nextNlMask&1 != 0 {
			// Boundary CRLF: next chunk's LF will be the delimiter
			newlineMaskOut &= ^(uint64(1) << 63)
		} else {
			// Isolated CR at position 63
			newlineMaskOut |= uint64(1) << 63
		}
	}

	return newlineMaskOut
}

// =============================================================================
// Quote and Separator Processing
// =============================================================================

// processQuotesAndSeparators processes masks to track quote state and invalidate
// separators inside quoted regions. Detects escaped double quotes ("") including
// those spanning chunk boundaries.
func processQuotesAndSeparators(quoteMask, sepMask, nextQuoteMask uint64, state *scanState) (quoteMaskOut, sepMaskOut uint64, hasDoubleQuote, boundaryDoubleQuote bool) {
	quoteMaskOut = quoteMask
	workQuote := quoteMask
	quoted := state.quoted
	initialQuoted := quoted

	// Step 1: Process quotes to detect and remove double quotes
	for workQuote != 0 {
		pos := bits.TrailingZeros64(workQuote)
		bit := uint64(1) << pos

		if quoted != 0 {
			// Inside quotes: check for escaped double quote
			if pos == simdChunkSize-1 && nextQuoteMask&1 != 0 {
				// Boundary double quote
				quoteMaskOut &^= uint64(1) << (simdChunkSize - 1)
				hasDoubleQuote = true
				boundaryDoubleQuote = true
			} else if pos < simdChunkSize-1 && workQuote&(uint64(1)<<(pos+1)) != 0 {
				// Adjacent double quote
				quoteMaskOut &^= uint64(3) << pos
				hasDoubleQuote = true
				workQuote &^= uint64(1) << (pos + 1)
			} else {
				// Closing quote
				quoted = 0
			}
		} else {
			// Opening quote
			quoted = ^uint64(0)
		}
		workQuote &^= bit
	}

	state.quoted = quoted

	// Step 2: Invalidate separators using prefix XOR on clean quote mask
	inQuote := quoteMaskOut
	inQuote ^= inQuote << 1
	inQuote ^= inQuote << 2
	inQuote ^= inQuote << 4
	inQuote ^= inQuote << 8
	inQuote ^= inQuote << 16
	inQuote ^= inQuote << 32

	if initialQuoted != 0 {
		inQuote = ^inQuote
	}

	sepMaskOut = sepMask &^ inQuote
	return
}

// invalidateNewlinesInQuotes removes newline bits that are inside quoted regions.
func invalidateNewlinesInQuotes(quoteMask, newlineMask uint64, state *scanState) uint64 {
	// Prefix XOR: inQuote[i] = 1 iff positions 0..i have odd number of quotes
	inQuote := quoteMask
	inQuote ^= inQuote << 1
	inQuote ^= inQuote << 2
	inQuote ^= inQuote << 4
	inQuote ^= inQuote << 8
	inQuote ^= inQuote << 16
	inQuote ^= inQuote << 32

	// If we started inside a quoted region, invert the mask
	if state.quoted != 0 {
		inQuote = ^inQuote
	}

	// Clear newline bits that are inside quoted regions
	return newlineMask &^ inQuote
}

// =============================================================================
// Buffer Scanning - Public API
// =============================================================================

// scanBuffer dispatches to the AVX-512 or scalar implementation.
func scanBuffer(buf []byte, separatorChar byte) *scanResult {
	if len(buf) == 0 {
		return &scanResult{}
	}
	if useAVX512 {
		return scanBufferAVX512(buf, separatorChar)
	}
	return scanBufferScalar(buf, separatorChar)
}

// =============================================================================
// Buffer Scanning - Scalar Implementation
// =============================================================================

// scalarMaskGenerator generates masks for a chunk using scalar operations.
type scalarMaskGenerator struct {
	separator byte
}

func (g *scalarMaskGenerator) generateFull(data []byte) chunkMasks {
	quote, sep, cr, nl := generateMasksScalar(data, g.separator)
	return chunkMasks{quote: quote, sep: sep, cr: cr, nl: nl}
}

func (g *scalarMaskGenerator) generatePadded(data []byte) (chunkMasks, int) {
	quote, sep, cr, nl, validBits := generateMasksPadded(data, g.separator)
	return chunkMasks{quote: quote, sep: sep, cr: cr, nl: nl}, validBits
}

// scanBufferScalar processes the buffer using scalar mask generation.
func scanBufferScalar(buf []byte, separatorChar byte) *scanResult {
	gen := &scalarMaskGenerator{separator: separatorChar}
	return scanBufferWithGenerator(buf, gen)
}

// =============================================================================
// Buffer Scanning - AVX-512 Implementation
// =============================================================================

// avx512MaskGenerator generates masks for a chunk using AVX-512 SIMD.
type avx512MaskGenerator struct {
	quoteCmp archsimd.Int8x64
	sepCmp   archsimd.Int8x64
	crCmp    archsimd.Int8x64
	nlCmp    archsimd.Int8x64
}

func newAVX512MaskGenerator(separator byte) *avx512MaskGenerator {
	return &avx512MaskGenerator{
		quoteCmp: archsimd.BroadcastInt8x64('"'),
		sepCmp:   archsimd.BroadcastInt8x64(int8(separator)),
		crCmp:    archsimd.BroadcastInt8x64('\r'),
		nlCmp:    archsimd.BroadcastInt8x64('\n'),
	}
}

func (g *avx512MaskGenerator) generateFull(data []byte) chunkMasks {
	quote, sep, cr, nl := generateMasksAVX512WithCmp(data, g.quoteCmp, g.sepCmp, g.crCmp, g.nlCmp)
	return chunkMasks{quote: quote, sep: sep, cr: cr, nl: nl}
}

func (g *avx512MaskGenerator) generatePadded(data []byte) (chunkMasks, int) {
	quote, sep, cr, nl, validBits := generateMasksPaddedWithCmp(data, g.quoteCmp, g.sepCmp, g.crCmp, g.nlCmp)
	return chunkMasks{quote: quote, sep: sep, cr: cr, nl: nl}, validBits
}

// scanBufferAVX512 processes the buffer using AVX-512 mask generation.
//
//go:noinline
func scanBufferAVX512(buf []byte, separatorChar byte) *scanResult {
	if len(buf) == 0 {
		return &scanResult{}
	}
	gen := newAVX512MaskGenerator(separatorChar)
	return scanBufferWithGenerator(buf, gen)
}

// =============================================================================
// Buffer Scanning - Unified Implementation
// =============================================================================

// maskGenerator abstracts mask generation for both SIMD and scalar paths.
type maskGenerator interface {
	generateFull(data []byte) chunkMasks
	generatePadded(data []byte) (chunkMasks, int)
}

// scanBufferWithGenerator processes the buffer using the provided mask generator.
// This unified implementation eliminates duplication between SIMD and scalar paths.
func scanBufferWithGenerator(buf []byte, gen maskGenerator) *scanResult {
	chunkCount := (len(buf) + simdChunkSize - 1) / simdChunkSize

	result := acquireScanResult(chunkCount)
	state := scanState{}

	curMasks, curValidBits := generateFirstChunkMasks(buf, gen, result)
	nextMasks := generateSecondChunkMasks(buf, chunkCount, gen, result)

	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		processChunk(chunkIdx, curMasks, nextMasks, curValidBits, &state, result)

		curMasks = nextMasks
		nextMasks, curValidBits = generateNextLookahead(buf, chunkIdx, chunkCount, gen, result)
	}

	result.finalQuoted = state.quoted
	return result
}

// acquireScanResult gets a pooled scanResult and initializes it for the given chunk count.
func acquireScanResult(chunkCount int) *scanResult {
	result := scanResultPool.Get().(*scanResult)
	result.reset()
	result.chunkCount = chunkCount
	initScanResultSlices(result, chunkCount)
	return result
}

// generateFirstChunkMasks generates masks for the first chunk of the buffer.
// Handles both full chunks and partial (padded) chunks.
func generateFirstChunkMasks(buf []byte, gen maskGenerator, result *scanResult) (chunkMasks, int) {
	if len(buf) >= simdChunkSize {
		return gen.generateFull(buf[0:simdChunkSize]), simdChunkSize
	}

	masks, validBits := gen.generatePadded(buf)
	result.lastChunkBits = validBits
	return masks, validBits
}

// generateSecondChunkMasks generates lookahead masks for the second chunk if it exists.
// Returns empty masks if there is no second chunk.
func generateSecondChunkMasks(buf []byte, chunkCount int, gen maskGenerator, result *scanResult) chunkMasks {
	if chunkCount <= 1 || len(buf) <= simdChunkSize {
		return chunkMasks{}
	}

	if len(buf) >= 2*simdChunkSize {
		return gen.generateFull(buf[simdChunkSize : 2*simdChunkSize])
	}

	masks, validBits := gen.generatePadded(buf[simdChunkSize:])
	if chunkCount == 2 {
		result.lastChunkBits = validBits
	}
	return masks
}

// generateNextLookahead generates masks for the chunk two positions ahead (lookahead).
// This enables processing current chunk while knowing what comes next.
func generateNextLookahead(buf []byte, chunkIdx, chunkCount int, gen maskGenerator, result *scanResult) (chunkMasks, int) {
	lookaheadIdx := chunkIdx + 2
	if lookaheadIdx >= chunkCount {
		return handleFinalChunkValidBits(buf, chunkIdx, chunkCount, result)
	}

	offset := lookaheadIdx * simdChunkSize
	remaining := len(buf) - offset

	if remaining >= simdChunkSize {
		return gen.generateFull(buf[offset : offset+simdChunkSize]), simdChunkSize
	}

	masks, validBits := gen.generatePadded(buf[offset:])
	result.lastChunkBits = validBits
	return masks, validBits
}

// handleFinalChunkValidBits computes valid bits when no more lookahead chunks exist.
func handleFinalChunkValidBits(buf []byte, chunkIdx, chunkCount int, result *scanResult) (chunkMasks, int) {
	validBits := simdChunkSize

	if chunkIdx+1 == chunkCount-1 && len(buf)%simdChunkSize != 0 {
		validBits = len(buf) % simdChunkSize
		result.lastChunkBits = validBits
	}

	return chunkMasks{}, validBits
}

// initScanResultSlices pre-sizes all slices for index-based assignment.
func initScanResultSlices(result *scanResult, chunkCount int) {
	result.quoteMasks = ensureUint64SliceCap(result.quoteMasks, chunkCount)
	result.separatorMasks = ensureUint64SliceCap(result.separatorMasks, chunkCount)
	result.newlineMasks = ensureUint64SliceCap(result.newlineMasks, chunkCount)
	result.chunkHasDQ = ensureBoolSliceCap(result.chunkHasDQ, chunkCount)
	result.chunkHasQuote = ensureBoolSliceCap(result.chunkHasQuote, chunkCount)
}

// =============================================================================
// Chunk Processing
// =============================================================================

// processChunk handles the main logic for a single chunk.
func processChunk(chunkIdx int, curMasks, nextMasks chunkMasks, validBits int, state *scanState, result *scanResult) {
	quoteMask := applyBoundaryQuoteSkip(curMasks.quote, state)
	newlineMask := normalizeCRLF(curMasks.cr, curMasks.nl, nextMasks.nl, validBits)

	if curMasks.cr != 0 {
		result.hasCR = true
	}

	if quoteMask == 0 {
		processChunkNoQuotes(chunkIdx, curMasks.sep, newlineMask, state, result)
		return
	}

	processChunkWithQuotes(chunkIdx, quoteMask, curMasks.sep, newlineMask, nextMasks.quote, state, result)
}

// applyBoundaryQuoteSkip removes the first quote if it's part of a boundary double quote.
func applyBoundaryQuoteSkip(quoteMask uint64, state *scanState) uint64 {
	if state.skipNextQuote && quoteMask&1 != 0 {
		quoteMask &= ^uint64(1)
	}
	state.skipNextQuote = false
	return quoteMask
}

// processChunkNoQuotes handles chunks without quote characters (fast path).
func processChunkNoQuotes(chunkIdx int, sepMask, newlineMask uint64, state *scanState, result *scanResult) {
	if state.quoted == 0 {
		result.quoteMasks[chunkIdx] = 0
		result.separatorMasks[chunkIdx] = sepMask
		result.newlineMasks[chunkIdx] = newlineMask
		result.separatorCount += bits.OnesCount64(sepMask)
		result.newlineCount += bits.OnesCount64(newlineMask)
	} else {
		result.quoteMasks[chunkIdx] = 0
		result.separatorMasks[chunkIdx] = 0
		result.newlineMasks[chunkIdx] = 0
	}
}

// processChunkWithQuotes handles chunks containing quote characters.
func processChunkWithQuotes(chunkIdx int, quoteMask, sepMask, newlineMask, nextQuoteMask uint64, state *scanState, result *scanResult) {
	initialQuoted := state.quoted

	quoteMaskOut, sepMaskOut, hasDoubleQuote, boundaryDoubleQuote := processQuotesAndSeparators(
		quoteMask, sepMask, nextQuoteMask, state,
	)

	if boundaryDoubleQuote {
		state.skipNextQuote = true
	}

	newlineMaskOut := invalidateNewlinesWithQuoteState(quoteMaskOut, newlineMask, initialQuoted, state)

	storeChunkResults(chunkIdx, quoteMaskOut, sepMaskOut, newlineMaskOut, hasDoubleQuote, result)
}

// invalidateNewlinesWithQuoteState removes newlines inside quotes using the initial quote state.
func invalidateNewlinesWithQuoteState(quoteMask, newlineMask, initialQuoted uint64, state *scanState) uint64 {
	endQuoted := state.quoted
	state.quoted = initialQuoted
	result := invalidateNewlinesInQuotes(quoteMask, newlineMask, state)
	state.quoted = endQuoted
	return result
}

// storeChunkResults writes the processed masks to the result.
func storeChunkResults(chunkIdx int, quoteMask, sepMask, newlineMask uint64, hasDoubleQuote bool, result *scanResult) {
	result.quoteMasks[chunkIdx] = quoteMask
	result.separatorMasks[chunkIdx] = sepMask
	result.newlineMasks[chunkIdx] = newlineMask

	if quoteMask != 0 {
		result.hasQuotes = true
		result.chunkHasQuote[chunkIdx] = true
	}
	if hasDoubleQuote {
		result.chunkHasDQ[chunkIdx] = true
	}

	result.separatorCount += bits.OnesCount64(sepMask)
	result.newlineCount += bits.OnesCount64(newlineMask)
}
