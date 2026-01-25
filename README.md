# go-simdcsv

> **⚠️ Experimental Project**: This is a showcase project for Go 1.26's experimental `simd/archsimd` package. The SIMD API is unstable and may change in future Go releases. Not recommended for production use.

A high-performance CSV parser for Go using SIMD (Single Instruction Multiple Data) instructions. Drop-in replacement for the standard library's `encoding/csv` package with performance improvements on AMD64 processors with AVX-512 support.

## Features

- **SIMD-accelerated parsing**: Uses Go 1.26's experimental `simd/archsimd` package with 256-bit vectors (requires AVX-512BW for `ToBits()` operation)
- **API compatible**: Drop-in replacement for `encoding/csv.Reader` and `encoding/csv.Writer`
- **RFC 4180 compliant**: Full support for quoted fields, escaped quotes, multiline fields, and CRLF normalization
- **Automatic fallback**: Gracefully falls back to scalar implementation on CPUs without AVX-512
- **Zero-copy API**: `ParseBytes` function for direct byte slice parsing without io.Reader overhead

## Requirements

- **Go 1.26+** with `GOEXPERIMENT=simd` build tag (experimental)
- **AMD64 architecture** (x86-64) only
- **AVX-512 support** for SIMD acceleration (AVX512F, AVX512BW, AVX512VL required)

### Limitations

- **Experimental SIMD API**: The `simd/archsimd` package is experimental and AMD64-specific. A portable high-level SIMD package is planned for future Go releases.
- **AVX-512 dependency**: Despite using 256-bit vectors (`Int8x32`), the `ToBits()` method requires AVX-512BW instruction (VPMOVB2M). This means SIMD acceleration is **not available** on:
  - Most CI environments (GitHub Actions `ubuntu-latest`, etc.)
  - CPUs without AVX-512 (Intel before Skylake-X, most AMD before Zen 4)
  - Apple Silicon (ARM64)
- **Memory**: Currently reads entire input into memory (streaming I/O planned)

> **Note**: On unsupported CPUs, the library automatically falls back to a scalar implementation with no SIMD acceleration.

## Installation

```bash
go get github.com/nnnkkk7/go-simdcsv
```

## Usage

### Basic Reading (Drop-in Replacement)

```go
package main

import (
    "fmt"
    "strings"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    data := "name,age,city\nAlice,30,Tokyo\nBob,25,Osaka\n"
    reader := csv.NewReader(strings.NewReader(data))

    records, err := reader.ReadAll()
    if err != nil {
        panic(err)
    }

    for _, record := range records {
        fmt.Println(record)
    }
}
```

### Zero-Copy Parsing

For maximum performance when you already have data in a byte slice:

```go
package main

import (
    "fmt"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    data := []byte("a,b,c\n1,2,3\n4,5,6\n")

    records, err := csv.ParseBytes(data, ',')
    if err != nil {
        panic(err)
    }

    for _, record := range records {
        fmt.Println(record)
    }
}
```

### Streaming API

Process records one at a time with a callback:

```go
package main

import (
    "fmt"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    data := []byte("name,value\nfoo,100\nbar,200\n")

    err := csv.ParseBytesStreaming(data, ',', func(record []string) error {
        fmt.Printf("Record: %v\n", record)
        return nil
    })
    if err != nil {
        panic(err)
    }
}
```

### Writing CSV

```go
package main

import (
    "os"

    csv "github.com/nnnkkk7/go-simdcsv"
)

func main() {
    writer := csv.NewWriter(os.Stdout)

    records := [][]string{
        {"name", "age", "city"},
        {"Alice", "30", "Tokyo"},
        {"Bob", "25", "Osaka"},
    }

    writer.WriteAll(records)
}
```

### Configuration Options

The Reader supports all standard `encoding/csv` options:

```go
reader := csv.NewReader(r)
reader.Comma = ';'              // Custom field delimiter
reader.Comment = '#'            // Comment character
reader.FieldsPerRecord = 3      // Expected fields per record (0 = auto-detect)
reader.LazyQuotes = true        // Allow bare quotes in unquoted fields
reader.TrimLeadingSpace = true  // Trim leading whitespace
reader.ReuseRecord = true       // Reuse record slice for performance
```

Extended options with `NewReaderWithOptions`:

```go
reader := csv.NewReaderWithOptions(r, csv.ReaderOptions{
    SkipBOM: true,  // Skip UTF-8 BOM if present
})
```


## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Raw Input  │ ──▶ │ scanBuffer() │ ──▶ │ parseBuffer()│ ──▶ │buildRecords()│ ──▶ [][]string
│   []byte    │     │  (bitmasks)  │     │  (positions) │     │  (strings)   │
└─────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
```

1. **`scanBuffer()`**: Scans input in 64-byte chunks using 256-bit SIMD vectors (`archsimd.Int8x32`). Detects positions of structural characters (`"`, `,`, `\n`, `\r`) and outputs bitmasks. Handles CRLF normalization and tracks quote state across chunk boundaries.

2. **`parseBuffer()`**: Iterates through bitmasks to determine field boundaries. Outputs `fieldInfo` (start offset, length) and `rowInfo` (field count per row). Correctly handles quoted fields containing commas or newlines.

3. **`buildRecords()`**: Extracts strings from byte positions. Applies double-quote unescaping (`""` → `"`) for fields that were marked during scanning.

## Building and Testing

### AMD64 Environment

```bash
# Build with SIMD support
GOEXPERIMENT=simd go build ./...

# Run tests
GOEXPERIMENT=simd go test -v ./...

# Run benchmarks
GOEXPERIMENT=simd go test -bench=. -benchmem
```

## Performance

Benchmarks comparing `go-simdcsv` against `encoding/csv` (run on AMD64 with AVX-512):

TODO: Add benchmark results here.

## Contributing

Contributions are welcome! Please open issues or pull requests on GitHub.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Known Issues

- **CI environments**: Most CI runners (GitHub Actions, etc.) do not have AVX-512 support. Tests pass using the scalar fallback, but SIMD acceleration is not tested in CI.
- **Apple Silicon**: Not supported. This library is AMD64-specific.
- **Go SIMD API stability**: The `simd/archsimd` package is experimental. Future Go releases may introduce breaking changes.
