//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - buffer size bounded by DefaultMaxInputSize (2GB)
package simdcsv

import "unsafe"

// ============================================================================
// Public API - Direct Parsing
// ============================================================================

// ParseBytes parses a byte slice directly (zero-copy).
// Returns all records extracted from the CSV data.
func ParseBytes(data []byte, comma rune) ([][]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	separator := byte(comma)
	sr := scanBuffer(data, separator)
	pr := parseBuffer(data, sr)
	records := buildRecords(data, pr, sr.hasCR)

	releaseParseResult(pr)
	releaseScanResult(sr)

	return records, nil
}

// ParseBytesStreaming parses data using a streaming callback function.
// The callback is invoked for each record. If it returns an error, parsing stops.
func ParseBytesStreaming(data []byte, comma rune, callback func([]string) error) error {
	if len(data) == 0 {
		return nil
	}

	separator := byte(comma)
	sr := scanBuffer(data, separator)
	pr := parseBuffer(data, sr)
	defer releaseParseResult(pr)
	defer releaseScanResult(sr)

	if pr == nil || len(pr.rows) == 0 {
		return nil
	}

	for _, row := range pr.rows {
		record := buildRecord(data, pr, row, sr.hasCR)
		if err := callback(record); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================================
// Internal - Record Building (for direct API)
// ============================================================================

// buildRecords converts a parseResult to [][]string.
// Optimizes memory by accumulating fields into a single buffer per record,
// then using zero-copy slicing after a single string conversion.
func buildRecords(buf []byte, pr *parseResult, hasCR bool) [][]string {
	if pr == nil || len(pr.rows) == 0 {
		return nil
	}

	records := make([][]string, len(pr.rows))

	// fieldEnds can be reused, but recordBuf must be unique per record for unsafe.String
	var fieldEnds []int

	for i, row := range pr.rows {
		var recordBuf []byte
		recordBuf, fieldEnds = accumulateFields(buf, pr, row, hasCR, recordBuf, fieldEnds[:0])
		records[i] = sliceFieldsFromBuffer(recordBuf, fieldEnds)
	}
	return records
}

// buildRecord builds a single record from a rowInfo (for streaming API).
func buildRecord(buf []byte, pr *parseResult, row rowInfo, hasCR bool) []string {
	recordBuf, fieldEnds := accumulateFields(buf, pr, row, hasCR, nil, nil)
	return sliceFieldsFromBuffer(recordBuf, fieldEnds)
}

// accumulateFields appends all field contents from a row into recordBuf.
// Returns the updated recordBuf and fieldEnds slice.
func accumulateFields(buf []byte, pr *parseResult, row rowInfo, hasCR bool, recordBuf []byte, fieldEnds []int) ([]byte, []int) {
	for i := 0; i < row.fieldCount; i++ {
		fieldIdx := row.firstField + i
		if fieldIdx >= len(pr.fields) {
			break
		}
		recordBuf = appendFieldContent(buf, pr.fields[fieldIdx], recordBuf, hasCR)
		fieldEnds = append(fieldEnds, len(recordBuf))
	}
	return recordBuf, fieldEnds
}

// sliceFieldsFromBuffer converts the accumulated buffer to individual field strings.
// Uses unsafe.String for zero-copy conversion. Caller must ensure recordBuf is not reused.
func sliceFieldsFromBuffer(recordBuf []byte, fieldEnds []int) []string {
	if len(recordBuf) == 0 {
		return make([]string, len(fieldEnds))
	}
	// Zero-copy string conversion - safe because recordBuf is unique per record
	str := unsafe.String(unsafe.SliceData(recordBuf), len(recordBuf))
	record := make([]string, len(fieldEnds))
	prevEnd := 0
	for i, end := range fieldEnds {
		record[i] = str[prevEnd:end]
		prevEnd = end
	}
	return record
}

// ============================================================================
// Internal - Field Content Extraction
// ============================================================================

// appendFieldContent appends field content to buffer with unescape and CRLF normalization.
// Policy: decides whether transformation is needed based on field metadata and content.
func appendFieldContent(buf []byte, field fieldInfo, recordBuf []byte, hasCR bool) []byte {
	content := extractFieldBytes(buf, field)
	if content == nil {
		return recordBuf
	}

	needsTransform := field.needsUnescape() || (hasCR && containsCRLFBytes(content))
	if !needsTransform {
		return append(recordBuf, content...)
	}

	return transformContent(content, recordBuf)
}

// extractFieldBytes returns the raw bytes for a field, handling bounds checking.
// Mechanism: pure extraction without transformation decisions.
func extractFieldBytes(buf []byte, field fieldInfo) []byte {
	if field.length == 0 {
		return nil
	}

	start := field.start
	end := field.start + field.length
	bufLen := uint32(len(buf))
	if start >= bufLen {
		return nil
	}
	if end > bufLen {
		end = bufLen
	}
	return buf[start:end]
}

// transformContent applies double-quote unescaping and CRLF normalization.
// Mechanism: pure transformation of bytes without policy decisions.
func transformContent(content, dst []byte) []byte {
	for i := 0; i < len(content); i++ {
		b := content[i]
		if b == '"' && i+1 < len(content) && content[i+1] == '"' {
			dst = append(dst, '"')
			i++
		} else if b == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			dst = append(dst, '\n')
			i++
		} else {
			dst = append(dst, b)
		}
	}
	return dst
}
