# just manual: https://github.com/casey/just#readme

_default:
	just --list

# Run all tests with nextest and cargo-llvm-cov
ci-test:
	#!/bin/bash -eux
	just build-testbed
	just cov

cov:
	scripts/cov.sh

quick-cov:
	SKIP_H2SPEC=1 scripts/cov.sh

# Run all tests with cargo nextest
test *args:
	just build-testbed
	RUST_BACKTRACE=1 cargo nextest run {{args}}

curl-tests *args:
	RUST_BACKTRACE=1 cargo nextest run --manifest-path test-crates/fluke-curl-tests/Cargo.toml {{args}}

build-testbed:
	cargo build --release --manifest-path test-crates/hyper-testbed/Cargo.toml

single-test *args:
	just test --no-capture {{args}}

bench *args:
	RUST_BACKTRACE=1 cargo bench {{args}} -- --plotting-backend plotters

h2spec *args:
	#!/bin/bash -eux
	export RUST_LOG="${RUST_LOG:-fluke=debug,fluke_hpack=info}"
	export RUST_BACKTRACE="${RUST_BACKTRACE:-1}"
	cargo run --manifest-path test-crates/fluke-h2spec/Cargo.toml -- {{args}}

check:
	#!/bin/bash -eu
		echo "Checking fluke"
	cargo clippy --all-targets --all-features

	# also for all subfolders of `test-crates/`
	for d in test-crates/*; do
		# if the Cargo.toml exists
		if [ -f "$d/Cargo.toml" ]; then
			echo "Checking $(basename "$d")"

		 pushd "$d" > /dev/null
		 cargo clippy --all-targets --all-features
		 popd > /dev/null
		fi
	done

ktls-sample:
	cargo run --manifest-path test-crates/fluke-tls-sample/Cargo.toml
