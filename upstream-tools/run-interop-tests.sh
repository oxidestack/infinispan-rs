#!/usr/bin/env bash
# Runs tests/topology_java_interop.rs: a Rust node and a REAL Infinispan 14.0.35.Final node
# (upstream-tools/topology-vectors/src/main/java/gen/InteropNode.java, JGroups 5.2.28.Final) form one
# cluster over TCP, in both join orders, replicate writes each way and transfer state to the joiner.
# The test builds its own JVM side (build-interop-node.sh), so a plain `cargo test` runs it too; this
# wrapper only adds --nocapture and a serial run. Needs JDK 17 and Maven; the tests bind free loopback ports.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
cargo test --test topology_java_interop -- --test-threads=1 --nocapture "$@"
