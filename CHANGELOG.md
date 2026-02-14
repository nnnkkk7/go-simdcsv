# Changelog

## [v0.1.0](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.14...v0.1.0) - 2026-02-14
- refactor: enhance field parsing by adding quote detection and optimization flags by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/72
- test: add benchmark tests for writer by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/74
- refactor: optimize quote detection and writing by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/75
- refactor: adjust SIMD thresholds and optimize quote detection in Writer by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/76
- feat: use PCLMULQDQ by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/77
- deps: upgrade go version to go1.26 by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/78
- ci: add renovate by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/79
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/80
- Release for v0.1.0 by @github-actions[bot] in https://github.com/nnnkkk7/go-simdcsv/pull/73

## [v0.1.0](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.14...v0.1.0) - 2026-02-14
- refactor: enhance field parsing by adding quote detection and optimization flags by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/72
- test: add benchmark tests for writer by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/74
- refactor: optimize quote detection and writing by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/75
- refactor: adjust SIMD thresholds and optimize quote detection in Writer by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/76
- feat: use PCLMULQDQ by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/77
- deps: upgrade go version to go1.26 by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/78
- ci: add renovate by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/79
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/80

## [v0.0.14](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.13...v0.0.14) - 2026-02-02
- refactor: optimize chunk processing and introduce fast path for no-quote scenarios by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/65
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/67
- refactor: use cached values by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/68
- refactor: enhance SIMD processing by adding cached separator values by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/69
- refactor: update SIMD processing to use 64-byte chunks by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/70
- test: tweak benchamark tests by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/71

## [v0.0.13](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.12...v0.0.13) - 2026-01-29
- chore: update CPU feature detection to use archsimd package by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/60
- perf: optimize SIMD mask processing with prefix XOR by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/62
- refactor: enhance record parsing efficiency by introducing no-quote path and optimizing memory usage by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/63
- perf: improve memory allocation and parsing efficiency  by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/64

## [v0.0.12](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.11...v0.0.12) - 2026-01-27
- docs: add performance comparison by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/54
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/56
- refactor: enhance error handling and improve code clarity by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/57
- chore: remove unused code by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/58

## [v0.0.12](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.11...v0.0.12) - 2026-01-27
- docs: add performance comparison by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/54
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/56
- refactor: enhance error handling and improve code clarity by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/57
- chore: remove unused code by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/58

## [v0.0.11](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.10...v0.0.11) - 2026-01-27
- ci: add cpu check by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/42
- ci: add pprof option by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/44
- refactor: enhance parsing efficiency with improved field and row estimation by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/45
- refactor: optimize chunk processing and mask handling in field parsing by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/46
- refactor: optimize field content handling and enhance SIMD scanning performance by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/47
- ci: fix benchmart option by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/48
- refactor: enhance record building and parsing efficiency with CR handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/49
- refactor: update SIMD scanning to utilize archsimd.Int8x64  by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/50
- refactor: optimize parseResult and scanResult pooling for improved me… by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/51
- refactor: adjust pooling capacities for parseResult and scanResult by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/52
- refactor: improve quoted field validation by leveraging parsed metadata by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/53

## [v0.0.10](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.9...v0.0.10) - 2026-01-27
- refactor: replace postProcChunks with chunkHasDQ for double quote by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/39
- refactor: implement object pooling for parseResult and scanResult by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/41

## [v0.0.9](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.8...v0.0.9) - 2026-01-27
- refactor: optimize field processing by simplifying raw field calculation by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/32
- refactor: enhance fieldInfo structure and optimize raw field calculations by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/34
- refactor: optimize record building with buffer reuse and improved field content handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/35
- ci: update go version by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/37
- refactor: unify fieldInfo structure and streamline raw field handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/36
- ci: fix actions and pins version by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/38

## [v0.0.8](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.7...v0.0.8) - 2026-01-26
- ci: add avx512 workflow by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/29
- ci: fix and rename wf for avx512 by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/31

## [v0.0.7](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.6...v0.0.7) - 2026-01-26
- refactor: update comments and constants for SIMD processing by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/24
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/26
- chore: fix formatting by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/27
- feat: enhance CSV reader with input size limit and new field processing functions by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/28

## [v0.0.6](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.5...v0.0.6) - 2026-01-25
- feat: implement SIMD-accelerated unescaping and quote handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/18
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/20
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/21
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/22

## [v0.0.6](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.5...v0.0.6) - 2026-01-25
- feat: implement SIMD-accelerated unescaping and quote handling by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/18
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/20
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/21
- docs: update readme by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/22

## [v0.0.5](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.4...v0.0.5) - 2026-01-23
- ci: fix goreleaser mode by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/11
- test: remove and integrate test by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/13
- deps: pin and update actions by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/14
- feat: add extended options and streaming parsing for CSV reader by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/15
- refactor: split functions and move them by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/16
- refactor: remove unnecessary stack by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/17

## [v0.0.4](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.3...v0.0.4) - 2026-01-20
- ci: fix how to handle draft release by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/9

## [v0.0.3](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.2...v0.0.3) - 2026-01-19
- ci: skip build by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/7

## [v0.0.2](https://github.com/nnnkkk7/go-simdcsv/compare/v0.0.1...v0.0.2) - 2026-01-19
- ci: fix ci trigger by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/4
- test: divide tests by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/5

## [v0.0.1](https://github.com/nnnkkk7/go-simdcsv/commits/v0.0.1) - 2026-01-19
- feat: init implementation by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/1
- ci: add relaser workflow by @nnnkkk7 in https://github.com/nnnkkk7/go-simdcsv/pull/2
