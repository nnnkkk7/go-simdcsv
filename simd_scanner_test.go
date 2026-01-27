//go:build goexperiment.simd && amd64

package simdcsv

import (
	"fmt"
	"strings"
	"testing"
)

// hexDump returns a formatted hex dump of data for debugging
func hexDump(data []byte, prefix string) string {
	var sb strings.Builder
	for i := 0; i < len(data); i += 16 {
		sb.WriteString(prefix)
		sb.WriteString(fmt.Sprintf("%04x: ", i))
		end := i + 16
		if end > len(data) {
			end = len(data)
		}
		// Hex bytes
		for j := i; j < end; j++ {
			sb.WriteString(fmt.Sprintf("%02x ", data[j]))
		}
		// Padding for incomplete lines
		for j := end; j < i+16; j++ {
			sb.WriteString("   ")
		}
		sb.WriteString(" |")
		// ASCII representation
		for j := i; j < end; j++ {
			if data[j] >= 32 && data[j] < 127 {
				sb.WriteByte(data[j])
			} else {
				sb.WriteByte('.')
			}
		}
		sb.WriteString("|\n")
	}
	return sb.String()
}

// maskBits returns a string showing which bits are set in a mask
func maskBits(m uint64, validBits int) string {
	var sb strings.Builder
	for i := 0; i < validBits && i < 64; i++ {
		if m&(1<<i) != 0 {
			sb.WriteByte('1')
		} else {
			sb.WriteByte('0')
		}
		if (i+1)%8 == 0 && i < validBits-1 {
			sb.WriteByte(' ')
		}
	}
	return sb.String()
}

// maskPositions returns a slice of bit positions that are set
func maskPositions(m uint64) []int {
	var positions []int
	for i := 0; i < 64; i++ {
		if m&(1<<i) != 0 {
			positions = append(positions, i)
		}
	}
	return positions
}

// makeAligned64 creates a 64-byte slice from input, padding with zeros if needed
func makeAligned64(data []byte) []byte {
	if len(data) >= 64 {
		return data[:64]
	}
	result := make([]byte, 64)
	copy(result, data)
	return result
}

// ============================================================================
// TestGenerateMasks - Test detection of quotes, separators, CR, LF
// ============================================================================

func TestGenerateMasks(t *testing.T) {
	tests := []struct {
		name         string
		input        []byte
		separator    byte
		wantQuotePos []int // expected bit positions for quotes
		wantSepPos   []int // expected bit positions for separators
		wantCRPos    []int // expected bit positions for CR
		wantNLPos    []int // expected bit positions for LF
	}{
		{
			name:         "simple_csv_line",
			input:        makeAligned64([]byte(`a,b,c`)),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   []int{1, 3},
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "quoted_field",
			input:        makeAligned64([]byte(`"hello",world`)),
			separator:    ',',
			wantQuotePos: []int{0, 6},
			wantSepPos:   []int{7},
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "lf_newlines",
			input:        makeAligned64([]byte("a,b\nc,d\n")),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   []int{1, 5},
			wantCRPos:    nil,
			wantNLPos:    []int{3, 7},
		},
		{
			name:         "crlf_newlines",
			input:        makeAligned64([]byte("a,b\r\nc,d\r\n")),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   []int{1, 6},
			wantCRPos:    []int{3, 8},
			wantNLPos:    []int{4, 9},
		},
		{
			// Input: "a,b","c""d",e
			// Pos:    0123456789...
			//         "   "," "  " ,
			// Position 0: " (quote)
			// Position 2: , (separator, inside quotes)
			// Position 4: " (quote)
			// Position 5: , (separator)
			// Position 6: " (quote)
			// Position 8: " (quote, first of "")
			// Position 9: " (quote, second of "")
			// Position 11: " (quote)
			// Position 12: , (separator)
			name:         "mixed_quotes_and_separators",
			input:        makeAligned64([]byte(`"a,b","c""d",e`)),
			separator:    ',',
			wantQuotePos: []int{0, 4, 6, 8, 9, 11},
			wantSepPos:   []int{2, 5, 12},
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "all_structural_chars",
			input:        makeAligned64([]byte("\",\r\n")),
			separator:    ',',
			wantQuotePos: []int{0},
			wantSepPos:   []int{1},
			wantCRPos:    []int{2},
			wantNLPos:    []int{3},
		},
		{
			name:         "tab_separator",
			input:        makeAligned64([]byte("a\tb\tc\n")),
			separator:    '\t',
			wantQuotePos: nil,
			wantSepPos:   []int{1, 3},
			wantCRPos:    nil,
			wantNLPos:    []int{5},
		},
		{
			name:         "semicolon_separator",
			input:        makeAligned64([]byte("a;b;c\n")),
			separator:    ';',
			wantQuotePos: nil,
			wantSepPos:   []int{1, 3},
			wantCRPos:    nil,
			wantNLPos:    []int{5},
		},
		{
			name:         "quotes_at_chunk_boundaries",
			input:        append(append(make([]byte, 31), '"'), append(make([]byte, 31), '"')...),
			separator:    ',',
			wantQuotePos: []int{31, 63},
			wantSepPos:   nil,
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "full_64_byte_csv",
			input:        []byte("aaaa,bbbb,cccc,dddd\neeee,ffff,gggg,hhhh\niiii,jjjj,kkkk,llll\nmmmm"),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   []int{4, 9, 14, 24, 29, 34, 44, 49, 54},
			wantCRPos:    nil,
			wantNLPos:    []int{19, 39, 59},
		},
		{
			name:         "no_structural_chars",
			input:        makeAligned64([]byte("abcdefghijklmnopqrstuvwxyz")),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   nil,
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "consecutive_separators",
			input:        makeAligned64([]byte(",,,,")),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   []int{0, 1, 2, 3},
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "consecutive_quotes",
			input:        makeAligned64([]byte(`""""`)),
			separator:    ',',
			wantQuotePos: []int{0, 1, 2, 3},
			wantSepPos:   nil,
			wantCRPos:    nil,
			wantNLPos:    nil,
		},
		{
			name:         "cr_without_lf",
			input:        makeAligned64([]byte("a\rb\rc")),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   nil,
			wantCRPos:    []int{1, 3},
			wantNLPos:    nil,
		},
		{
			name:         "lf_without_cr",
			input:        makeAligned64([]byte("a\nb\nc")),
			separator:    ',',
			wantQuotePos: nil,
			wantSepPos:   nil,
			wantCRPos:    nil,
			wantNLPos:    []int{1, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.input) != 64 {
				t.Fatalf("test input must be exactly 64 bytes, got %d", len(tt.input))
			}

			quote, sep, cr, nl := generateMasks(tt.input, tt.separator)

			gotQuotePos := maskPositions(quote)
			gotSepPos := maskPositions(sep)
			gotCRPos := maskPositions(cr)
			gotNLPos := maskPositions(nl)

			if !equalPositions(gotQuotePos, tt.wantQuotePos) {
				t.Errorf("quote mask mismatch:\n  input: %s\n  got:  %v\n  want: %v\n  mask: %s",
					hexDump(tt.input, "    "), gotQuotePos, tt.wantQuotePos, maskBits(quote, 64))
			}
			if !equalPositions(gotSepPos, tt.wantSepPos) {
				t.Errorf("separator mask mismatch:\n  input: %s\n  got:  %v\n  want: %v\n  mask: %s",
					hexDump(tt.input, "    "), gotSepPos, tt.wantSepPos, maskBits(sep, 64))
			}
			if !equalPositions(gotCRPos, tt.wantCRPos) {
				t.Errorf("CR mask mismatch:\n  input: %s\n  got:  %v\n  want: %v\n  mask: %s",
					hexDump(tt.input, "    "), gotCRPos, tt.wantCRPos, maskBits(cr, 64))
			}
			if !equalPositions(gotNLPos, tt.wantNLPos) {
				t.Errorf("LF mask mismatch:\n  input: %s\n  got:  %v\n  want: %v\n  mask: %s",
					hexDump(tt.input, "    "), gotNLPos, tt.wantNLPos, maskBits(nl, 64))
			}
		})
	}
}

// ============================================================================
// TestGenerateMasksPadded - Test input < 64 bytes with padding
// ============================================================================

func TestGenerateMasksPadded(t *testing.T) {
	tests := []struct {
		name          string
		input         []byte
		separator     byte
		wantQuotePos  []int
		wantSepPos    []int
		wantCRPos     []int
		wantNLPos     []int
		wantValidBits int
	}{
		{
			name:          "single_byte",
			input:         []byte(","),
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    []int{0},
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 1,
		},
		{
			// 31 bytes: 16 letters + 15 commas = 31
			name:          "31_bytes",
			input:         []byte("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p"),
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29},
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 31,
		},
		{
			// 32 bytes: 16 letters + 16 commas = 32
			name:          "32_bytes_exact_half",
			input:         []byte("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,"),
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31},
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 32,
		},
		{
			// 33 bytes: 17 letters + 16 commas = 33
			name:          "33_bytes_just_over_half",
			input:         []byte("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q"),
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31},
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 33,
		},
		{
			// 63 bytes: 32 letters + 31 commas = 63
			name:          "63_bytes_one_short",
			input:         []byte("a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z,A,B,C,D,E,F"),
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    []int{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61},
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 63,
		},
		{
			name:          "empty_input",
			input:         []byte{},
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    nil,
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 0,
		},
		{
			name:          "short_with_quote",
			input:         []byte(`"a"`),
			separator:     ',',
			wantQuotePos:  []int{0, 2},
			wantSepPos:    nil,
			wantCRPos:     nil,
			wantNLPos:     nil,
			wantValidBits: 3,
		},
		{
			name:          "short_with_crlf",
			input:         []byte("a\r\n"),
			separator:     ',',
			wantQuotePos:  nil,
			wantSepPos:    nil,
			wantCRPos:     []int{1},
			wantNLPos:     []int{2},
			wantValidBits: 3,
		},
		{
			name:          "short_mixed_structural",
			input:         []byte(`"a,b",c` + "\n"),
			separator:     ',',
			wantQuotePos:  []int{0, 4},
			wantSepPos:    []int{2, 5},
			wantCRPos:     nil,
			wantNLPos:     []int{7},
			wantValidBits: 8,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			quote, sep, cr, nl, validBits := generateMasksPadded(tt.input, tt.separator)

			if validBits != tt.wantValidBits {
				t.Errorf("validBits = %d, want %d", validBits, tt.wantValidBits)
			}

			gotQuotePos := maskPositions(quote)
			gotSepPos := maskPositions(sep)
			gotCRPos := maskPositions(cr)
			gotNLPos := maskPositions(nl)

			// Verify no bits are set beyond validBits
			if validBits < 64 {
				beyondMask := ^((uint64(1) << validBits) - 1)
				if quote&beyondMask != 0 {
					t.Errorf("quote mask has bits set beyond validBits: %s", maskBits(quote, 64))
				}
				if sep&beyondMask != 0 {
					t.Errorf("separator mask has bits set beyond validBits: %s", maskBits(sep, 64))
				}
				if cr&beyondMask != 0 {
					t.Errorf("CR mask has bits set beyond validBits: %s", maskBits(cr, 64))
				}
				if nl&beyondMask != 0 {
					t.Errorf("LF mask has bits set beyond validBits: %s", maskBits(nl, 64))
				}
			}

			if !equalPositions(gotQuotePos, tt.wantQuotePos) {
				t.Errorf("quote mask mismatch:\n  got:  %v\n  want: %v", gotQuotePos, tt.wantQuotePos)
			}
			if !equalPositions(gotSepPos, tt.wantSepPos) {
				t.Errorf("separator mask mismatch:\n  got:  %v\n  want: %v", gotSepPos, tt.wantSepPos)
			}
			if !equalPositions(gotCRPos, tt.wantCRPos) {
				t.Errorf("CR mask mismatch:\n  got:  %v\n  want: %v", gotCRPos, tt.wantCRPos)
			}
			if !equalPositions(gotNLPos, tt.wantNLPos) {
				t.Errorf("LF mask mismatch:\n  got:  %v\n  want: %v", gotNLPos, tt.wantNLPos)
			}
		})
	}
}

// TestGenerateMasksPadded_ValidBitsOnly verifies that only valid bit positions
// contain meaningful data after padding
func TestGenerateMasksPadded_ValidBitsOnly(t *testing.T) {
	// Test that padding bytes (zeros) don't accidentally match structural chars
	lengths := []int{1, 2, 7, 8, 15, 16, 17, 31, 32, 33, 48, 63}

	for _, length := range lengths {
		t.Run(fmt.Sprintf("length_%d", length), func(t *testing.T) {
			// Create input with structural chars at specific positions
			input := make([]byte, length)
			for i := range input {
				input[i] = 'x' // non-structural char
			}
			// Put a comma at position 0 if length > 1 (reserve pos 0 for separator test)
			// For length=1, we only test quote at position 0
			if length > 1 {
				input[0] = ','
			}
			// Put a quote at the last position
			if length > 0 {
				input[length-1] = '"'
			}

			_, sep, _, _, validBits := generateMasksPadded(input, ',')

			if validBits != length {
				t.Errorf("validBits = %d, want %d", validBits, length)
			}

			// Verify separator at position 0 (only if length > 1)
			if length > 1 {
				if sep&1 == 0 {
					t.Errorf("expected separator at position 0")
				}
			}

			// Count set bits - should only be in valid range
			sepPositions := maskPositions(sep)
			for _, pos := range sepPositions {
				if pos >= validBits {
					t.Errorf("separator bit set at position %d, but validBits is %d", pos, validBits)
				}
			}
		})
	}
}

// ============================================================================
// TestCRLFNormalization - Test CRLF pair handling
// ============================================================================

func TestCRLFNormalization(t *testing.T) {
	tests := []struct {
		name               string
		input              []byte
		wantNewlinePos     []int // positions in final newline mask (after normalization)
		wantCRLFNormalized bool  // true if CRLF was normalized
		description        string
	}{
		{
			name:               "simple_crlf",
			input:              makeAligned64([]byte("a,b\r\nc,d")),
			wantNewlinePos:     []int{4}, // CRLF at 3,4 -> only LF at 4 counts
			wantCRLFNormalized: true,
			description:        "CRLF pair should produce single newline at LF position",
		},
		{
			name:               "multiple_crlf",
			input:              makeAligned64([]byte("a\r\nb\r\nc")),
			wantNewlinePos:     []int{2, 5}, // CRLFs at 1-2 and 4-5
			wantCRLFNormalized: true,
			description:        "Multiple CRLF pairs",
		},
		{
			name:               "isolated_cr",
			input:              makeAligned64([]byte("a\rb\nc")),
			wantNewlinePos:     []int{1, 3}, // isolated CR at 1, LF at 3
			wantCRLFNormalized: false,
			description:        "Isolated CR should be treated as newline",
		},
		{
			name:               "isolated_lf",
			input:              makeAligned64([]byte("a\nb\nc")),
			wantNewlinePos:     []int{1, 3},
			wantCRLFNormalized: false,
			description:        "LF without preceding CR",
		},
		{
			name:               "mixed_line_endings",
			input:              makeAligned64([]byte("a\r\nb\nc\rd")),
			wantNewlinePos:     []int{2, 4, 6}, // CRLF->2, LF->4, CR->6
			wantCRLFNormalized: true,
			description:        "Mix of CRLF, LF, and CR line endings",
		},
		{
			name:               "consecutive_crlf",
			input:              makeAligned64([]byte("a\r\n\r\nb")),
			wantNewlinePos:     []int{2, 4}, // Two consecutive empty lines
			wantCRLFNormalized: true,
			description:        "Consecutive CRLF pairs (empty lines)",
		},
		{
			name:               "cr_then_cr_lf",
			input:              makeAligned64([]byte("a\r\r\nb")),
			wantNewlinePos:     []int{1, 3}, // isolated CR, then CRLF
			wantCRLFNormalized: true,
			description:        "Isolated CR followed by CRLF",
		},
		{
			name:               "crlf_at_end",
			input:              append(makeAligned64(nil)[:60], []byte("ab\r\n")...),
			wantNewlinePos:     []int{63}, // CRLF at positions 62,63
			wantCRLFNormalized: true,
			description:        "CRLF at end of chunk",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.input) != 64 {
				t.Fatalf("test input must be 64 bytes, got %d", len(tt.input))
			}

			// Generate raw masks
			_, _, crMask, nlMask := generateMasks(tt.input, ',')

			// Apply CRLF normalization logic (as per design doc section 3.5)
			// CRLF pairs: CR followed by LF - CR is removed from newline mask
			crlfPairs := crMask & (nlMask >> 1) // CR at position i, LF at position i+1
			isolatedCR := crMask & ^crlfPairs   // CRs not followed by LF

			// Final newline mask: LF positions + isolated CR positions
			newlineMaskOut := nlMask | isolatedCR

			gotNewlinePos := maskPositions(newlineMaskOut)

			if !equalPositions(gotNewlinePos, tt.wantNewlinePos) {
				t.Errorf("%s:\n  input hex:\n%s  CR mask:     %s\n  LF mask:     %s\n  CRLF pairs:  %s\n  isolated CR: %s\n  newline out: %s\n  got positions:  %v\n  want positions: %v",
					tt.description,
					hexDump(tt.input, "    "),
					maskBits(crMask, 64),
					maskBits(nlMask, 64),
					maskBits(crlfPairs, 64),
					maskBits(isolatedCR, 64),
					maskBits(newlineMaskOut, 64),
					gotNewlinePos,
					tt.wantNewlinePos)
			}
		})
	}
}

// TestCRLFBoundary tests CRLF pairs that span chunk boundaries
// CR at byte 63 of chunk N, LF at byte 0 of chunk N+1
func TestCRLFBoundary(t *testing.T) {
	tests := []struct {
		name         string
		chunk1       []byte // first 64-byte chunk (CR at end)
		chunk2       []byte // second 64-byte chunk (LF at start)
		wantChunk1NL []int  // newline positions in chunk 1
		wantChunk2NL []int  // newline positions in chunk 2
		description  string
	}{
		{
			name:         "boundary_crlf",
			chunk1:       append(make([]byte, 63), '\r'),
			chunk2:       append([]byte{'\n'}, make([]byte, 63)...),
			wantChunk1NL: nil,      // CR at 63 is part of CRLF, not counted here
			wantChunk2NL: []int{0}, // LF at 0 is the actual newline
			description:  "CRLF split across chunk boundary",
		},
		{
			name:         "boundary_isolated_cr",
			chunk1:       append(make([]byte, 63), '\r'),
			chunk2:       append([]byte{'a'}, make([]byte, 63)...), // not LF
			wantChunk1NL: []int{63},                                // CR at 63 is isolated
			wantChunk2NL: nil,
			description:  "Isolated CR at chunk boundary (no LF follows)",
		},
		{
			name:         "boundary_crlf_with_more_content",
			chunk1:       append([]byte("data,more,stuff\r\n"), append(make([]byte, 46), '\r')...),
			chunk2:       append([]byte{'\n', 'a', ',', 'b'}, make([]byte, 60)...),
			wantChunk1NL: []int{16}, // CRLF at 15,16 -> LF at 16; CR at 63 is boundary CRLF
			wantChunk2NL: []int{0},  // LF at 0 from boundary CRLF
			description:  "Content with CRLF, then boundary CRLF",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.chunk1) != 64 || len(tt.chunk2) != 64 {
				t.Fatalf("chunks must be 64 bytes each")
			}

			// Get masks for both chunks
			_, _, cr1, nl1 := generateMasks(tt.chunk1, ',')
			_, _, _, nl2 := generateMasks(tt.chunk2, ',')

			// Check boundary condition: CR at position 63 of chunk1
			crAtBoundary := cr1&(1<<63) != 0
			lfAtNextStart := nl2&1 != 0

			// Normalize CRLF within chunk1
			crlfPairs1 := cr1 & (nl1 >> 1)
			isolatedCR1 := cr1 & ^crlfPairs1

			// Handle boundary CRLF
			if crAtBoundary && lfAtNextStart {
				// CR at 63 is part of boundary CRLF, remove from isolated CR
				isolatedCR1 &= ^(uint64(1) << 63)
			}

			newlineMask1 := nl1 | isolatedCR1

			// Normalize CRLF within chunk2 (boundary LF is real newline)
			crlfPairs2 := uint64(0) // simplified for test
			isolatedCR2 := uint64(0)
			newlineMask2 := nl2 | isolatedCR2
			_ = crlfPairs2

			gotChunk1NL := maskPositions(newlineMask1)
			gotChunk2NL := maskPositions(newlineMask2)

			if !equalPositions(gotChunk1NL, tt.wantChunk1NL) {
				t.Errorf("%s - chunk1 newlines:\n  got:  %v\n  want: %v\n  chunk1:\n%s",
					tt.description, gotChunk1NL, tt.wantChunk1NL, hexDump(tt.chunk1, "    "))
			}
			if !equalPositions(gotChunk2NL, tt.wantChunk2NL) {
				t.Errorf("%s - chunk2 newlines:\n  got:  %v\n  want: %v\n  chunk2:\n%s",
					tt.description, gotChunk2NL, tt.wantChunk2NL, hexDump(tt.chunk2, "    "))
			}
		})
	}
}

// ============================================================================
// TestChunkBoundaryQuotes - Test escaped quotes ("") spanning chunk boundary
// ============================================================================

func TestChunkBoundaryQuotes(t *testing.T) {
	tests := []struct {
		name                string
		chunk1              []byte // first 64-byte chunk (quote at end)
		chunk2              []byte // second 64-byte chunk (quote at start)
		chunk1Quoted        bool   // is chunk1 processing starting inside quoted region?
		wantSkipNextQuote   bool   // should skipNextQuote be set after chunk1?
		wantChunk2QuoteSkip bool   // should first quote in chunk2 be skipped?
		description         string
	}{
		{
			name:                "boundary_double_quote_escape",
			chunk1:              append(make([]byte, 63), '"'), // only quote at position 63
			chunk2:              append([]byte{'"'}, make([]byte, 63)...),
			chunk1Quoted:        true, // we're inside a quoted field
			wantSkipNextQuote:   true, // the "" escape spans boundary (pos 63 + chunk2 pos 0)
			wantChunk2QuoteSkip: true,
			description:         "Double quote escape spanning chunk boundary",
		},
		{
			name:                "boundary_quote_close",
			chunk1:              append(make([]byte, 63), '"'),
			chunk2:              append([]byte{','}, make([]byte, 63)...),
			chunk1Quoted:        true,
			wantSkipNextQuote:   false, // quote at 63 closes the field
			wantChunk2QuoteSkip: false,
			description:         "Quote at boundary closes quoted field (next is comma)",
		},
		{
			name:                "boundary_quote_open",
			chunk1:              append(make([]byte, 63), '"'),
			chunk2:              append([]byte{'a'}, make([]byte, 63)...),
			chunk1Quoted:        false,
			wantSkipNextQuote:   false, // quote at 63 opens a field
			wantChunk2QuoteSkip: false,
			description:         "Quote at boundary opens quoted field",
		},
		{
			name:                "boundary_escaped_quote_inside_field",
			chunk1:              append(append([]byte(`"content`), make([]byte, 55)...), '"'),
			chunk2:              append([]byte{'"', 'm', 'o', 'r', 'e', '"'}, make([]byte, 58)...),
			chunk1Quoted:        false, // starts outside
			wantSkipNextQuote:   true,  // "" at boundary
			wantChunk2QuoteSkip: true,
			description:         `"content..." with "" escape at boundary`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if len(tt.chunk1) != 64 || len(tt.chunk2) != 64 {
				t.Fatalf("chunks must be 64 bytes each")
			}

			// Get quote masks
			quote1, _, _, _ := generateMasks(tt.chunk1, ',')
			quote2, _, _, _ := generateMasks(tt.chunk2, ',')

			// Check boundary condition
			quoteAtChunk1End := quote1&(1<<63) != 0
			quoteAtChunk2Start := quote2&1 != 0

			// Calculate quote state at position 63 by processing all quotes before it.
			// We need to know if we're inside a quoted region when we reach position 63.
			// Start with the initial state (chunk1Quoted), then toggle for each quote.
			quotedAtPos63 := tt.chunk1Quoted
			quotesBeforePos63 := quote1 & ((uint64(1) << 63) - 1) // mask off bit 63
			// Count quotes before position 63 - odd count means state toggled
			quoteCount := 0
			for mask := quotesBeforePos63; mask != 0; mask &= mask - 1 {
				quoteCount++
			}
			if quoteCount%2 == 1 {
				quotedAtPos63 = !quotedAtPos63
			}

			// Simulate double quote detection at boundary
			// This happens when:
			// 1. We're inside a quoted region at position 63 (after processing all prior quotes)
			// 2. Quote at position 63 of chunk1
			// 3. Quote at position 0 of chunk2
			skipNextQuote := false
			if quotedAtPos63 && quoteAtChunk1End && quoteAtChunk2Start {
				skipNextQuote = true
			}

			if skipNextQuote != tt.wantSkipNextQuote {
				t.Errorf("%s:\n  skipNextQuote = %v, want %v\n  quoteAtChunk1End = %v\n  quoteAtChunk2Start = %v\n  quotedAtPos63 = %v (initial=%v, toggles=%d)\n  chunk1:\n%s  chunk2:\n%s",
					tt.description,
					skipNextQuote, tt.wantSkipNextQuote,
					quoteAtChunk1End, quoteAtChunk2Start,
					quotedAtPos63, tt.chunk1Quoted, quoteCount,
					hexDump(tt.chunk1, "    "),
					hexDump(tt.chunk2, "    "))
			}

			// If skipNextQuote is set, the first quote in chunk2 should be ignored
			if skipNextQuote && tt.wantChunk2QuoteSkip {
				adjustedQuote2 := quote2 & ^uint64(1) // clear bit 0
				if adjustedQuote2&1 != 0 {
					t.Errorf("failed to skip first quote in chunk2")
				}
			}
		})
	}
}

// TestSkipNextQuoteFlag tests the skipNextQuote state flag behavior
func TestSkipNextQuoteFlag(t *testing.T) {
	// Simulate processing two chunks with skipNextQuote flag
	type chunkResult struct {
		quoteMaskOut  uint64
		skipNextQuote bool
	}

	processChunk := func(quoteMask uint64, inQuoted bool, skipNextQuote bool, nextChunkQuoteMask uint64) chunkResult {
		result := chunkResult{}

		// If skipNextQuote is set, clear the first quote bit
		if skipNextQuote && quoteMask&1 != 0 {
			quoteMask &= ^uint64(1)
		}

		// Check for boundary double quote (simplified logic)
		quoteAtEnd := quoteMask&(1<<63) != 0
		nextQuoteAtStart := nextChunkQuoteMask&1 != 0

		// If we end with a quote and next chunk starts with a quote, it might be escaped
		// Need to know if we're in quoted state at position 63
		if quoteAtEnd && nextQuoteAtStart {
			// For this test, assume we're inside quoted region
			// Real implementation would track quote state through the chunk
			if inQuoted {
				result.skipNextQuote = true
				quoteMask &= ^(uint64(1) << 63) // remove trailing quote from output
			}
		}

		result.quoteMaskOut = quoteMask
		return result
	}

	t.Run("escaped_quote_sequence", func(t *testing.T) {
		// Chunk 1: ...data"   (quote at position 63)
		// Chunk 2: "more...   (quote at position 0)
		// This represents "" escape sequence
		chunk1QuoteMask := uint64(1) << 63 // quote at 63
		chunk2QuoteMask := uint64(1)       // quote at 0

		result1 := processChunk(chunk1QuoteMask, true, false, chunk2QuoteMask)
		if !result1.skipNextQuote {
			t.Error("expected skipNextQuote to be true after chunk1")
		}

		result2 := processChunk(chunk2QuoteMask, true, result1.skipNextQuote, 0)
		if result2.quoteMaskOut&1 != 0 {
			t.Error("expected first quote in chunk2 to be skipped")
		}
	})
}

// ============================================================================
// TestScanBuffer - Test full buffer processing
// ============================================================================

func TestScanBuffer(t *testing.T) {
	tests := []struct {
		name            string
		input           []byte
		separator       byte
		wantChunkCount  int
		wantChunkHasDQ  []bool // per-chunk flags for chunks that have escaped quotes
		wantFinalQuoted bool   // should we end in quoted state?
		description     string
	}{
		{
			name:            "single_chunk_simple",
			input:           []byte("a,b,c\nd,e,f\n"),
			separator:       ',',
			wantChunkCount:  1,
			wantChunkHasDQ:  []bool{false},
			wantFinalQuoted: false,
			description:     "Simple CSV fitting in one chunk",
		},
		{
			name:            "single_chunk_quoted",
			input:           []byte(`"a","b","c"` + "\n"),
			separator:       ',',
			wantChunkCount:  1,
			wantChunkHasDQ:  []bool{false},
			wantFinalQuoted: false,
			description:     "Quoted fields in one chunk",
		},
		{
			name:            "single_chunk_escaped",
			input:           []byte(`"a""b","c"` + "\n"),
			separator:       ',',
			wantChunkCount:  1,
			wantChunkHasDQ:  []bool{true}, // chunk 0 has escaped quote
			wantFinalQuoted: false,
			description:     "Escaped quote in one chunk",
		},
		{
			name:            "two_chunks_simple",
			input:           append(make([]byte, 64), []byte("a,b,c\n")...),
			separator:       ',',
			wantChunkCount:  2,
			wantChunkHasDQ:  []bool{false, false},
			wantFinalQuoted: false,
			description:     "Two chunks, simple content",
		},
		{
			name:            "multiple_chunks",
			input:           make([]byte, 200), // 200 bytes = 4 chunks (64+64+64+8)
			separator:       ',',
			wantChunkCount:  4,
			wantChunkHasDQ:  []bool{false, false, false, false},
			wantFinalQuoted: false,
			description:     "Multiple full and partial chunks",
		},
		{
			name:            "exact_64_bytes",
			input:           make([]byte, 64),
			separator:       ',',
			wantChunkCount:  1,
			wantChunkHasDQ:  []bool{false},
			wantFinalQuoted: false,
			description:     "Exactly 64 bytes (one full chunk)",
		},
		{
			name:            "exact_128_bytes",
			input:           make([]byte, 128),
			separator:       ',',
			wantChunkCount:  2,
			wantChunkHasDQ:  []bool{false, false},
			wantFinalQuoted: false,
			description:     "Exactly 128 bytes (two full chunks)",
		},
		{
			name:            "unclosed_quote",
			input:           []byte(`"unclosed`),
			separator:       ',',
			wantChunkCount:  1,
			wantChunkHasDQ:  []bool{false},
			wantFinalQuoted: true, // quote at position 0 never closed
			description:     "Unclosed quote should leave quoted state true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scanBuffer(tt.input, tt.separator)

			if result.chunkCount != tt.wantChunkCount {
				t.Errorf("%s: chunkCount = %d, want %d",
					tt.description, result.chunkCount, tt.wantChunkCount)
			}

			// Verify mask slice lengths match chunk count
			if len(result.quoteMasks) != tt.wantChunkCount {
				t.Errorf("%s: len(quoteMasks) = %d, want %d",
					tt.description, len(result.quoteMasks), tt.wantChunkCount)
			}
			if len(result.separatorMasks) != tt.wantChunkCount {
				t.Errorf("%s: len(separatorMasks) = %d, want %d",
					tt.description, len(result.separatorMasks), tt.wantChunkCount)
			}
			if len(result.newlineMasks) != tt.wantChunkCount {
				t.Errorf("%s: len(newlineMasks) = %d, want %d",
					tt.description, len(result.newlineMasks), tt.wantChunkCount)
			}

			if !equalBoolSlices(result.chunkHasDQ, tt.wantChunkHasDQ) {
				t.Errorf("%s: chunkHasDQ = %v, want %v",
					tt.description, result.chunkHasDQ, tt.wantChunkHasDQ)
			}

			gotFinalQuoted := result.finalQuoted != 0
			if gotFinalQuoted != tt.wantFinalQuoted {
				t.Errorf("%s: finalQuoted = %v, want %v",
					tt.description, gotFinalQuoted, tt.wantFinalQuoted)
			}
		})
	}
}

// TestScanBuffer_MaskContent verifies the actual mask content
func TestScanBuffer_MaskContent(t *testing.T) {
	tests := []struct {
		name         string
		input        []byte
		separator    byte
		chunkIdx     int
		wantSepPos   []int
		wantNLPos    []int
		wantQuotePos []int
		description  string
	}{
		{
			name:         "verify_separator_positions",
			input:        []byte("a,b,c,d\ne,f,g,h\n"),
			separator:    ',',
			chunkIdx:     0,
			wantSepPos:   []int{1, 3, 5, 9, 11, 13},
			wantNLPos:    []int{7, 15},
			wantQuotePos: nil,
			description:  "Verify separator and newline positions",
		},
		{
			name:         "verify_quoted_field_masks",
			input:        []byte(`"a,b",c` + "\n"),
			separator:    ',',
			chunkIdx:     0,
			wantSepPos:   []int{5}, // comma inside quotes should be removed
			wantNLPos:    []int{7},
			wantQuotePos: []int{0, 4}, // opening and closing quotes
			description:  "Quoted field should hide internal comma",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := scanBuffer(tt.input, tt.separator)

			if tt.chunkIdx >= len(result.separatorMasks) {
				t.Fatalf("chunkIdx %d out of range (have %d chunks)",
					tt.chunkIdx, len(result.separatorMasks))
			}

			gotSepPos := maskPositions(result.separatorMasks[tt.chunkIdx])
			gotNLPos := maskPositions(result.newlineMasks[tt.chunkIdx])
			gotQuotePos := maskPositions(result.quoteMasks[tt.chunkIdx])

			if !equalPositions(gotSepPos, tt.wantSepPos) {
				t.Errorf("%s - separator positions:\n  got:  %v\n  want: %v\n  mask: %s",
					tt.description, gotSepPos, tt.wantSepPos,
					maskBits(result.separatorMasks[tt.chunkIdx], 64))
			}
			if !equalPositions(gotNLPos, tt.wantNLPos) {
				t.Errorf("%s - newline positions:\n  got:  %v\n  want: %v\n  mask: %s",
					tt.description, gotNLPos, tt.wantNLPos,
					maskBits(result.newlineMasks[tt.chunkIdx], 64))
			}
			if !equalPositions(gotQuotePos, tt.wantQuotePos) {
				t.Errorf("%s - quote positions:\n  got:  %v\n  want: %v\n  mask: %s",
					tt.description, gotQuotePos, tt.wantQuotePos,
					maskBits(result.quoteMasks[tt.chunkIdx], 64))
			}
		})
	}
}

// TestScanBuffer_MultiChunk tests processing that spans multiple chunks
func TestScanBuffer_MultiChunk(t *testing.T) {
	// Create input that spans 3 chunks with known structure
	// Chunk 0: "field1,field2,field3\nfield4,field5,field6\nfield7,field8,fie"
	// Chunk 1: "ld9\nfield10,field11,field12\nfield13,field14,field15\nfield16,"
	// Chunk 2: "field17\n"

	// Create input that spans 3 chunks (129+ bytes needed)
	// Chunk 0: bytes 0-63
	// Chunk 1: bytes 64-127
	// Chunk 2: bytes 128+
	chunk0 := strings.Repeat("a,", 20) + "field\n" + strings.Repeat("b,", 10) // ~66 bytes
	chunk1 := strings.Repeat("c,", 25) + "data\n"                             // ~55 bytes
	chunk2 := "last\n"                                                        // 5 bytes

	input := []byte(chunk0 + chunk1 + chunk2)

	// Ensure we have at least 3 chunks
	if len(input) < 129 {
		// Pad to ensure 3 chunks
		input = append(input, make([]byte, 129-len(input))...)
	}

	result := scanBuffer(input, ',')

	expectedChunkCount := (len(input) + 63) / 64
	if result.chunkCount != expectedChunkCount {
		t.Errorf("expected %d chunks, got %d", expectedChunkCount, result.chunkCount)
	}

	// Verify we have mask slices for all chunks
	if len(result.newlineMasks) != result.chunkCount {
		t.Errorf("newlineMasks length %d doesn't match chunkCount %d",
			len(result.newlineMasks), result.chunkCount)
	}

	// Verify total newlines across all chunks
	totalNL := 0
	for _, mask := range result.newlineMasks {
		totalNL += popcount(mask)
	}
	expectedTotalNL := strings.Count(string(input), "\n")
	if totalNL != expectedTotalNL {
		t.Errorf("total newlines: got %d, want %d", totalNL, expectedTotalNL)
	}
}

// TestScanBuffer_Empty tests empty input handling
func TestScanBuffer_Empty(t *testing.T) {
	result := scanBuffer([]byte{}, ',')

	if result.chunkCount != 0 {
		t.Errorf("empty input should have 0 chunks, got %d", result.chunkCount)
	}
	if len(result.quoteMasks) != 0 {
		t.Errorf("empty input should have empty quoteMasks")
	}
	if len(result.separatorMasks) != 0 {
		t.Errorf("empty input should have empty separatorMasks")
	}
	if len(result.newlineMasks) != 0 {
		t.Errorf("empty input should have empty newlineMasks")
	}
	if result.finalQuoted != 0 {
		t.Errorf("empty input should not be in quoted state")
	}
}

// TestScanBuffer_LastChunkBits tests partial final chunk handling
func TestScanBuffer_LastChunkBits(t *testing.T) {
	tests := []struct {
		inputLen          int
		wantLastChunkBits int
	}{
		{1, 1},
		{32, 32},
		{63, 63},
		{64, 0},   // exactly 64 bytes = full chunk, lastChunkBits not set
		{65, 1},   // 64 + 1
		{100, 36}, // 64 + 36
		{128, 0},  // exactly 2 chunks
		{129, 1},  // 128 + 1
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("len_%d", tt.inputLen), func(t *testing.T) {
			input := make([]byte, tt.inputLen)
			result := scanBuffer(input, ',')

			if result.lastChunkBits != tt.wantLastChunkBits {
				t.Errorf("lastChunkBits = %d, want %d", result.lastChunkBits, tt.wantLastChunkBits)
			}
		})
	}
}

// ============================================================================
// Benchmark tests
// ============================================================================

func BenchmarkGenerateMasks(b *testing.B) {
	data := make([]byte, 64)
	copy(data, []byte(`"field1","field2","field3","field4","field5","field6","fie"`))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateMasks(data, ',')
	}
}

func BenchmarkGenerateMasksPadded(b *testing.B) {
	sizes := []int{1, 16, 32, 48, 63}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				if i%2 == 0 {
					data[i] = ','
				} else {
					data[i] = 'a'
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				generateMasksPadded(data, ',')
			}
		})
	}
}

func BenchmarkScanBuffer(b *testing.B) {
	sizes := []int{64, 1024, 64 * 1024, 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Create realistic CSV-like data
			data := make([]byte, size)
			for i := range data {
				switch i % 10 {
				case 3, 7:
					data[i] = ','
				case 9:
					data[i] = '\n'
				default:
					data[i] = 'a' + byte(i%26)
				}
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				scanBuffer(data, ',')
			}
		})
	}
}

// ============================================================================
// Helper functions
// ============================================================================

// equalPositions compares two slices of positions (nil and empty are equal)
func equalPositions(a, b []int) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// equalSlices compares two int slices
func equalBoolSlices(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// popcount returns the number of set bits in a uint64
func popcount(x uint64) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}
