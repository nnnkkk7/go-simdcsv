//go:build goexperiment.simd && amd64

//nolint:gosec // G115: Integer conversions are safe - buffer size bounded by DefaultMaxInputSize (2GB)
package simdcsv

// ParseBytes parses a byte slice directly (zero-copy).
// This function runs scanBuffer and parseBuffer processing and returns all records.
func ParseBytes(data []byte, comma rune) ([][]string, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Scan: Structural analysis using SIMD (generates bitmasks)
	separatorChar := byte(comma)
	sr := scanBuffer(data, separatorChar)

	// Parse: Extract fields and rows from scan result
	pr := parseBuffer(data, sr)

	// Build: Convert parseResult to [][]string
	records := buildRecords(data, pr)

	// Release parseResult back to pool
	releaseParseResult(pr)

	return records, nil
}

// ParseBytesStreaming parses data using a streaming callback function.
// The callback is invoked for each record parsed from the input.
// If the callback returns an error, parsing stops and that error is returned.
func ParseBytesStreaming(data []byte, comma rune, callback func([]string) error) error {
	if len(data) == 0 {
		return nil
	}

	// Scan: Structural analysis using SIMD (generates bitmasks)
	separatorChar := byte(comma)
	sr := scanBuffer(data, separatorChar)

	// Parse: Extract fields and rows from scan result
	pr := parseBuffer(data, sr)
	defer releaseParseResult(pr)

	if pr == nil || len(pr.rows) == 0 {
		return nil
	}

	// Build: Invoke callback for each record
	for _, row := range pr.rows {
		record := buildRecord(data, pr, row)
		if err := callback(record); err != nil {
			return err
		}
	}
	return nil
}

// buildRecords converts a parseResult to [][]string using the standard library pattern.
// This optimizes memory allocation by:
// 1. Accumulating all field content into a single recordBuffer per record
// 2. Converting to string once per record
// 3. Zero-copy slicing to create individual field strings
func buildRecords(buf []byte, pr *parseResult) [][]string {
	if pr == nil || len(pr.rows) == 0 {
		return nil
	}

	records := make([][]string, len(pr.rows))

	// Shared buffers (reused across records)
	var recordBuffer []byte
	var fieldEnds []int

	for rowIdx, row := range pr.rows {
		// Reset buffers (reuse capacity)
		recordBuffer = recordBuffer[:0]
		fieldEnds = fieldEnds[:0]

		// Phase 1: Accumulate all field content into recordBuffer
		for i := 0; i < row.fieldCount; i++ {
			fieldIdx := row.firstField + i
			if fieldIdx >= len(pr.fields) {
				break
			}
			field := pr.fields[fieldIdx]
			appendFieldToBuffer(buf, field, &recordBuffer)
			fieldEnds = append(fieldEnds, len(recordBuffer))
		}

		// Phase 2: Single string conversion + zero-copy slicing
		str := string(recordBuffer)
		record := make([]string, len(fieldEnds))
		prevEnd := 0
		for i, end := range fieldEnds {
			record[i] = str[prevEnd:end]
			prevEnd = end
		}
		records[rowIdx] = record
	}
	return records
}

// appendFieldToBuffer appends field content to buffer with inline unescape and CRLF normalization.
func appendFieldToBuffer(buf []byte, field fieldInfo, recordBuffer *[]byte) {
	if field.length == 0 {
		return
	}

	// Get field content safely
	start := field.start
	end := field.start + field.length
	bufLen := uint32(len(buf))
	if start >= bufLen {
		return
	}
	if end > bufLen {
		end = bufLen
	}
	content := buf[start:end]

	// Check if transformation is needed
	needsTransform := field.needsUnescape() || containsCRLFBytes(content)
	if !needsTransform {
		// Fast path: append as-is
		*recordBuffer = append(*recordBuffer, content...)
		return
	}

	// Slow path: inline unescape ("" -> ") and CRLF normalization (\r\n -> \n)
	for i := 0; i < len(content); i++ {
		b := content[i]
		if b == '"' && i+1 < len(content) && content[i+1] == '"' {
			*recordBuffer = append(*recordBuffer, '"')
			i++ // skip next quote
		} else if b == '\r' && i+1 < len(content) && content[i+1] == '\n' {
			*recordBuffer = append(*recordBuffer, '\n')
			i++ // skip \n
		} else {
			*recordBuffer = append(*recordBuffer, b)
		}
	}
}

// buildRecord builds a single record from a rowInfo (for streaming API).
func buildRecord(buf []byte, pr *parseResult, row rowInfo) []string {
	var recordBuffer []byte
	var fieldEnds []int

	for i := 0; i < row.fieldCount; i++ {
		fieldIdx := row.firstField + i
		if fieldIdx >= len(pr.fields) {
			break
		}
		field := pr.fields[fieldIdx]
		appendFieldToBuffer(buf, field, &recordBuffer)
		fieldEnds = append(fieldEnds, len(recordBuffer))
	}

	str := string(recordBuffer)
	record := make([]string, len(fieldEnds))
	prevEnd := 0
	for i, end := range fieldEnds {
		record[i] = str[prevEnd:end]
		prevEnd = end
	}
	return record
}
