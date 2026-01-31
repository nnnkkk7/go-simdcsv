//go:build goexperiment.simd && amd64

package simdcsv

import (
	"math/bits"
	"simd/archsimd"
	"sync"
	"unsafe"
)

// bytesToInt8Slice converts a byte slice to an int8 slice without copying.
// This enables use of LoadInt8xNSlice functions which are safer than pointer casts.
func bytesToInt8Slice(b []byte) []int8 {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Slice((*int8)(unsafe.Pointer(unsafe.SliceData(b))), len(b))
}

// useAVX512 indicates whether AVX-512 instructions are available at runtime.
var useAVX512 bool

// Cached broadcast values for fixed characters and separators (initialized in init()).
var (
	// AVX-512 (64-byte) cached values
	cachedQuoteCmp archsimd.Int8x64
	cachedCrCmp    archsimd.Int8x64
	cachedNlCmp    archsimd.Int8x64
	cachedSepCmp   [cachedSepCmpCount]archsimd.Int8x64
)

// SIMD processing constants.
const (
	simdChunkSize       = 64  // bytes per AVX-512 iteration
	simdHalfChunk       = 32  // bytes for half chunk (AVX2 size)
	simdMinThreshold    = 32  // minimum size for SIMD benefit
	avgFieldLenEstimate = 15  // estimated avg field length for preallocation
	avgRowLenEstimate   = 80  // estimated avg row length for preallocation
	cachedSepCmpCount   = 256 // number of cached separator broadcast values
)

func init() {
	useAVX512 = archsimd.X86.AVX512()
	if useAVX512 {
		// Pre-broadcast all byte values to avoid repeated BroadcastInt8x64 calls
		for i := 0; i < cachedSepCmpCount; i++ {
			// #nosec G115 -- i is bounded [0,255], intentional two's-complement mapping.
			cachedSepCmp[i] = archsimd.BroadcastInt8x64(int8(i))
		}
		cachedQuoteCmp = cachedSepCmp['"']
		cachedCrCmp = cachedSepCmp['\r']
		cachedNlCmp = cachedSepCmp['\n']
	}
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
// 4096 chunks = ~256KB input, reducing allocations for typical CSV sizes.
const scanResultPoolCapacity = 4096

// scanResultLargeThreshold retains large scanResults to avoid repeated allocations across GCs.
// 16384 chunks = ~1MB input.
const scanResultLargeThreshold = 16384

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

// scanResultLargeCache retains a single large scanResult across GC cycles.
// This prevents repeated large allocations when processing files > 1MB,
// as sync.Pool may evict large objects during GC.
var scanResultLargeCache struct {
	mu sync.Mutex
	sr *scanResult
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

// release returns the scanResult to the pool for reuse.
// Large results (>= scanResultLargeThreshold) are cached separately to survive GC.
func (sr *scanResult) release() {
	if sr == nil {
		return
	}

	sr.reset()

	// Cache large results separately to prevent GC eviction
	if cap(sr.quoteMasks) >= scanResultLargeThreshold {
		scanResultLargeCache.mu.Lock()
		shouldCache := scanResultLargeCache.sr == nil ||
			cap(scanResultLargeCache.sr.quoteMasks) < cap(sr.quoteMasks)
		if shouldCache {
			scanResultLargeCache.sr = sr
			scanResultLargeCache.mu.Unlock()
			return
		}
		scanResultLargeCache.mu.Unlock()
	}

	scanResultPool.Put(sr)
}

// =============================================================================
// Slice Utilities
// =============================================================================

// ensureUint64SliceCap ensures slice has at least required length.
// Reuses existing capacity when possible.
func ensureUint64SliceCap(s []uint64, required int) []uint64 {
	if cap(s) >= required {
		return s[:required]
	}
	// Allocate exact size to avoid over-allocation for small inputs
	return make([]uint64, required)
}

// ensureBoolSliceCap ensures slice has at least required length (cleared).
// Reuses existing capacity when possible.
func ensureBoolSliceCap(s []bool, required int) []bool {
	if cap(s) >= required {
		s = s[:required]
		clear(s)
		return s
	}
	// Allocate exact size to avoid over-allocation for small inputs
	return make([]bool, required)
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
// Uses cached broadcast values for fixed characters (quote, CR, NL) to avoid
// repeated BroadcastInt8x64 calls.
func generateMasksAVX512(data []byte, separator byte) (quote, sep, cr, nl uint64) {
	return generateMasksAVX512WithCmp(data, cachedQuoteCmp, cachedSepCmp[separator], cachedCrCmp, cachedNlCmp)
}

// generateMasksAVX512WithCmp generates masks reusing pre-broadcasted comparators.
func generateMasksAVX512WithCmp(data []byte, quoteCmp, sepCmp, crCmp, nlCmp archsimd.Int8x64) (quote, sep, cr, nl uint64) {
	chunk := archsimd.LoadInt8x64Slice(bytesToInt8Slice(data))
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
// Uses LoadInt8x64SlicePart to safely load partial chunks without manual padding.
func generateMasksPaddedWithCmp(data []byte, quoteCmp, sepCmp, crCmp, nlCmp archsimd.Int8x64) (quote, sep, cr, nl uint64, validBits int) {
	validBits = len(data)
	if validBits == 0 {
		return 0, 0, 0, 0, 0
	}

	// SlicePart safely loads partial data, zero-filling unused lanes
	chunk := archsimd.LoadInt8x64SlicePart(bytesToInt8Slice(data))
	quote = chunk.Equal(quoteCmp).ToBits()
	sep = chunk.Equal(sepCmp).ToBits()
	cr = chunk.Equal(crCmp).ToBits()
	nl = chunk.Equal(nlCmp).ToBits()

	// Mask out bits beyond valid data
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
		quoteCmp: cachedQuoteCmp,
		sepCmp:   cachedSepCmp[separator],
		crCmp:    cachedCrCmp,
		nlCmp:    cachedNlCmp,
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

	sc := bufferScanContext{
		buf:        buf,
		gen:        gen,
		result:     result,
		chunkCount: chunkCount,
	}

	curMasks, curValidBits := sc.generateFirstChunkMasks()
	nextMasks := sc.generateSecondChunkMasks()

	for chunkIdx := 0; chunkIdx < chunkCount; chunkIdx++ {
		processChunk(chunkIdx, curMasks, nextMasks, curValidBits, &state, result)

		curMasks = nextMasks
		nextMasks, curValidBits = sc.generateNextLookahead(chunkIdx)
	}

	result.finalQuoted = state.quoted
	return result
}

type bufferScanContext struct {
	buf        []byte
	gen        maskGenerator
	result     *scanResult
	chunkCount int
}

// acquireScanResult gets a pooled scanResult and initializes it for the given chunk count.
func acquireScanResult(chunkCount int) *scanResult {
	if chunkCount >= scanResultLargeThreshold {
		scanResultLargeCache.mu.Lock()
		result := scanResultLargeCache.sr
		if result != nil && cap(result.quoteMasks) >= chunkCount {
			scanResultLargeCache.sr = nil
			scanResultLargeCache.mu.Unlock()
			result.reset()
			result.chunkCount = chunkCount
			initScanResultSlices(result, chunkCount)
			return result
		}
		scanResultLargeCache.mu.Unlock()
	}

	result := scanResultPool.Get().(*scanResult)
	result.reset()
	result.chunkCount = chunkCount
	initScanResultSlices(result, chunkCount)
	return result
}

// generateFirstChunkMasks generates masks for the first chunk of the buffer.
// Handles both full chunks and partial (padded) chunks.
func (sc *bufferScanContext) generateFirstChunkMasks() (chunkMasks, int) {
	if len(sc.buf) >= simdChunkSize {
		return sc.gen.generateFull(sc.buf[0:simdChunkSize]), simdChunkSize
	}

	masks, validBits := sc.gen.generatePadded(sc.buf)
	sc.result.lastChunkBits = validBits
	return masks, validBits
}

// generateSecondChunkMasks generates lookahead masks for the second chunk if it exists.
// Returns empty masks if there is no second chunk.
func (sc *bufferScanContext) generateSecondChunkMasks() chunkMasks {
	if sc.chunkCount <= 1 || len(sc.buf) <= simdChunkSize {
		return chunkMasks{}
	}

	if len(sc.buf) >= 2*simdChunkSize {
		return sc.gen.generateFull(sc.buf[simdChunkSize : 2*simdChunkSize])
	}

	masks, validBits := sc.gen.generatePadded(sc.buf[simdChunkSize:])
	if sc.chunkCount == 2 {
		sc.result.lastChunkBits = validBits
	}
	return masks
}

// generateNextLookahead generates masks for the chunk two positions ahead (lookahead).
// This enables processing current chunk while knowing what comes next.
func (sc *bufferScanContext) generateNextLookahead(chunkIdx int) (chunkMasks, int) {
	lookaheadIdx := chunkIdx + 2
	if lookaheadIdx >= sc.chunkCount {
		return sc.handleFinalChunkValidBits(chunkIdx)
	}

	offset := lookaheadIdx * simdChunkSize
	remaining := len(sc.buf) - offset

	if remaining >= simdChunkSize {
		return sc.gen.generateFull(sc.buf[offset : offset+simdChunkSize]), simdChunkSize
	}

	masks, validBits := sc.gen.generatePadded(sc.buf[offset:])
	sc.result.lastChunkBits = validBits
	return masks, validBits
}

// handleFinalChunkValidBits computes valid bits when no more lookahead chunks exist.
func (sc *bufferScanContext) handleFinalChunkValidBits(chunkIdx int) (chunkMasks, int) {
	validBits := simdChunkSize

	if chunkIdx+1 == sc.chunkCount-1 && len(sc.buf)%simdChunkSize != 0 {
		validBits = len(sc.buf) % simdChunkSize
		sc.result.lastChunkBits = validBits
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
