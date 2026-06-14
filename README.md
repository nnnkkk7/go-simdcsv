# go-simdcsv

[![Go 1.26+](https://img.shields.io/badge/Go-1.26%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Go Report Card](https://goreportcard.com/badge/github.com/nnnkkk7/go-simdcsv)](https://goreportcard.com/report/github.com/nnnkkk7/go-simdcsv)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

SIMD-accelerated CSV parser for Go — drop-in replacement for `encoding/csv` with AVX-512 optimization.

> **Experimental**: Requires Go 1.26+ with `GOEXPERIMENT=simd`. The SIMD API is unstable and may change. Not recommended for production use.

## Quick Start

```go
import csv "github.com/nnnkkk7/go-simdcsv"

reader := csv.NewReader(strings.NewReader("name,age\nAlice,30\nBob,25"))
records, err := reader.ReadAll()
if err != nil {
    log.Fatal(err)
}
// records: [["name", "age"], ["Alice", "30"], ["Bob", "25"]]
```

Same API as `encoding/csv` — just change the import.

## Features

| Feature | Description |
|---------|-------------|
| **API Compatible** | Drop-in replacement for `encoding/csv.Reader` and `Writer` |
| **Full CSV Support** | Quoted fields, escaped quotes (`""`), multiline fields, CRLF handling |
| **Auto Fallback** | Gracefully falls back to scalar on non-AVX-512 CPUs |
| **Direct Byte API** | `ParseBytes()` and `ParseBytesStreaming()` for `[]byte` input |

## Installation

```bash
go get github.com/nnnkkk7/go-simdcsv
```

## Usage

### Reading

```go
// All at once
records, err := csv.NewReader(r).ReadAll()

// Record by record
reader := csv.NewReader(r)
for {
    record, err := reader.Read()
    if err == io.EOF { break }
    if err != nil { return err }
    // process record
}

// Direct byte parsing (fastest)
records, err := csv.ParseBytes(data, ',')

// Streaming callback
csv.ParseBytesStreaming(data, ',', func(record []string) error {
    return processRecord(record)
})
```

### Writing

```go
writer := csv.NewWriter(w)
writer.WriteAll(records)
```

### Configuration

All standard `encoding/csv` options are supported:

```go
reader := csv.NewReader(r)
reader.Comma = ';'              // Field delimiter (default: ',')
reader.Comment = '#'            // Comment character
reader.LazyQuotes = true        // Allow bare quotes
reader.TrimLeadingSpace = true  // Trim leading whitespace
reader.ReuseRecord = true       // Reuse slice for performance
reader.FieldsPerRecord = 3      // Expected fields (0 = auto-detect, -1 = variable)
```

Extended options:

```go
reader := csv.NewReaderWithOptions(r, csv.ReaderOptions{
    SkipBOM: true,  // Skip UTF-8 BOM if present
})
```

## Performance

Benchmarks on AMD EPYC 9R14 with AVX-512 (Go 1.26, `GOEXPERIMENT=simd`). See [Contributing](#contributing) for the CI setup.

### ReadAll Throughput

| Content | Size | encoding/csv | go-simdcsv | Comparison |
|---------|------|--------------|------------|------------|
| Unquoted fields | 10K rows | 228 MB/s | 309 MB/s | **+35% faster** |
| Unquoted fields | 100K rows | 214 MB/s | 288 MB/s | **+35% faster** |
| Realistic (40% quoted) | 100K rows | ? MB/s | ? MB/s | ? |
| Heavily quoted fields | 100K rows | ? MB/s | ? MB/s | ? |

**Note:** Realistic rows mix 4 quoted fields (containing commas) with 6 plain fields per row — the case where PCLMULQDQ prefix-XOR fires on partial data. Heavily quoted uses all-quoted fields with embedded commas (worst case). Values marked `?` are measured on the AVX-512 CI runner.

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Raw Input  │ ──▶ │ scanBuffer() │ ──▶ │ parseBuffer()│ ──▶ │buildRecords()│ ──▶ [][]string
│   []byte    │     │  (bitmasks)  │     │  (positions) │     │  (strings)   │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

| Stage | Function | Description |
|-------|----------|-------------|
| **Scan** | `scanBuffer()` | SIMD scanning in 64-byte chunks. Detects `"`, `,`, `\n`, `\r` positions as bitmasks. |
| **Parse** | `parseBuffer()` | Iterates bitmasks to find field boundaries. Outputs `fieldInfo` and `rowInfo`. |
| **Build** | `buildRecords()` | Extracts strings from positions. Applies `""` → `"` unescaping. |

## Requirements

| Requirement | Details |
|-------------|---------|
| **Go** | 1.26+ with `GOEXPERIMENT=simd` |
| **Architecture** | AMD64 (x86-64) only |
| **SIMD Acceleration** | AVX-512 (F, BW, VL) required |

### Without AVX-512

The library **still works** but falls back to scalar implementation (no speedup). This includes:
- Most CI environments (GitHub Actions `ubuntu-latest`, etc.)
- Intel CPUs before Skylake-X
- AMD CPUs before Zen 4
- Apple Silicon (ARM64)

## Building & Testing

```bash
# Build
GOEXPERIMENT=simd go build ./...

# Test
GOEXPERIMENT=simd go test -v ./...

# Benchmark
GOEXPERIMENT=simd go test -bench=. -benchmem
```

## Known Limitations

- **Experimental API**: `simd/archsimd` may have breaking changes in future Go releases
- **Quoted fields**: Currently slower than `encoding/csv` for heavily quoted content

## Contributing

Contributions are welcome! Please open issues or pull requests on GitHub.

### CI

This repository runs two GitHub Actions workflows:

| Workflow | Trigger | Runner | SIMD acceleration |
|----------|---------|--------|-------------------|
| [`ci.yml`](.github/workflows/ci.yml) | Every pull request | GitHub-hosted `ubuntu-latest` | No (scalar fallback) |
| [`ci-avx512.yml`](.github/workflows/ci-avx512.yml) | Manual only | Self-hosted (`avx512`) | Yes |

Pull request checks use `ubuntu-latest`, which typically lacks AVX-512. Tests still pass because the library falls back to the scalar path when SIMD instructions are unavailable.

AVX-512 benchmarks and SIMD validation run only on the self-hosted runner via **AVX-512 CI**. Trigger it manually from the Actions tab when you need real SIMD test and benchmark results.

## License

MIT License - see [LICENSE](LICENSE) file for details.
