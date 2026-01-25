//go:build goexperiment.simd && amd64

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
	return buildRecords(data, pr), nil
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

// buildRecords converts a parseResult to [][]string.
func buildRecords(buf []byte, pr *parseResult) [][]string {
	if pr == nil || len(pr.rows) == 0 {
		return nil
	}

	records := make([][]string, len(pr.rows))
	for rowIdx, row := range pr.rows {
		records[rowIdx] = buildRecord(buf, pr, row)
	}
	return records
}

// buildRecord builds a single record from a rowInfo.
func buildRecord(buf []byte, pr *parseResult, row rowInfo) []string {
	record := make([]string, row.fieldCount)
	for i := 0; i < row.fieldCount; i++ {
		fieldIdx := row.firstField + i
		if fieldIdx >= len(pr.fields) {
			break
		}
		record[i] = extractField(buf, pr.fields[fieldIdx])
	}
	return record
}
