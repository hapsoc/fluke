# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1](https://github.com/bearcove/fluke/compare/fluke-v0.1.0...fluke-v0.1.1) - 2024-03-20

### Added
- Upgrade dependencies

### Fixed
- fix several low-hanging bugs: re io/read tasks

### Other
- hapsoc => fluke
- Fix h2spec http2/6.4/3
- Fix h2spec/6.5.2/1
- Fix h2spec http2/6.5/3
- More h2spec cases
- Close connection explicitly in h2spec
- Upgrade tokio-uring
- mhh
- Fix more cases
- well every time I lose silly time to this I improve debug logging, so.
- mh
- Uhm
- better debug implementation for Frame
- Remove read module, move it back into server.rs
- Remove write module altogether
- Move all writing out of write
- Retire H2ConnEvent
- mhmh
- Move settings acknowledgement inside read
- Deprecate H2ConnEvent::RstStream
- Move goaway writing away from write
- Move ping to write_frame
- Introduce write_frame
- Give out_scratch to h2readcontext
- Get rid of write task
- Fix some flow-control cases
- Fix PING tests
- Fix more tests
- Oh joy, h2spec is wrong
- More test fixes
- Fix more stream state cases
- more stream state things
- Fix some stream state cases
- Rename StreamStage to StreamState
- Implement RstStream
- Fix hpack tests
- looking good
- No more compile errors woo
- Simplifying headers/trailing reading + being more rigorous with conneciton errors
- no continuation state
- Upgrade more deps
- Upgrade pretty-hex
- Upgrade deps
- Graceful GOAWAY handling
- Set minimum rust version for crates with async fn in trait
- Bump some dependencies
- Remove TAIT
- Bump dependencies
- Opt out of async_fn_in_trait warnings
- Move fluke-h2spec somewhere else
- Move curl tests into their own crate
- Simplify H2Result code further, try sccache.exe on Windows
- Have a few methods return H2Result, which clarifies the control flow
- Fix http2/5.1/5
- Introduce 'write' module
- Move types around
- Bring state names closer to RFC
- First stab at 5.1.2
- todo => debug, don't crash on window_update for known streams
- Don't send GO_AWAY when receiving a connection-wide window update
- Send GO_AWAY if we get a stray WINDOW_UPDATE
- Extract process_frame method out of H2ReadContext::work
- release

## [0.1.0](https://github.com/bearcove/fluke/releases/tag/fluke-v0.1.0) - 2023-10-03

### Other

- Fix fluke-maybe-uring cargo-publish showstoppers
- Rebrand to fluke
