#!/usr/bin/env bash
# Builds the JVM side of tests/topology_java_interop.rs into <cargo target dir>/interop/ (dependencies resolved
# through Maven from topology-vectors/pom.xml, InteropNode.java and MockServer's VersionedWrapper
# compiled from the pinned submodule) and prints two lines on stdout: the JAVA_HOME of a JDK 17, then
# the class path. Only InteropNode.java (and what it references) is compiled, so an unrelated vector
# generator that does not build cannot break it. Needs JDK 17 and Maven.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TV="$ROOT/upstream-tools/topology-vectors"
# Into cargo's own target directory (the test passes its CARGO_TARGET_TMPDIR), never a `target/` beside this
# crate's Cargo.toml: as a workspace member of another project such a directory looks like the
# crate's build output, and `cargo clean --manifest-path <crate>/Cargo.toml` aimed at it wipes the whole
# workspace's target/ instead.
if [[ -n ${INFINISPAN_RS_INTEROP_DIR:-} ]]; then
  OUT="$INFINISPAN_RS_INTEROP_DIR"
else
  TARGET="$(cargo metadata --format-version 1 --no-deps --manifest-path "$ROOT/Cargo.toml" | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')"
  OUT="${TARGET:?cargo metadata gave no target_directory}/interop"
fi
MS="${MOCKSERVER_SRC:-$ROOT/../../upstream/mockserver/mockserver}"  # the `mockserver` directory of a MockServer 8.0.0 checkout; the default is the layout of a consumer that has this crate under crates/

JAVA_HOME="$(/usr/libexec/java_home -v 17)"  # JDK 25 removed Subject.getSubject, which infinispan-core 14 calls
export JAVA_HOME
export PATH="$JAVA_HOME/bin:$PATH"
mkdir -p "$OUT/libs"
if ! ls "$OUT"/libs/infinispan-core-*.jar >/dev/null 2>&1; then
  mvn -q -f "$TV/pom.xml" dependency:copy-dependencies -DoutputDirectory="$OUT/libs" >&2
fi
rm -rf "$OUT/cls"
mkdir -p "$OUT/cls"
javac -nowarn -cp "$OUT/libs/*" -sourcepath "$TV/src/main/java" -d "$OUT/cls" "$TV/src/main/java/gen/InteropNode.java" \
  "$MS/mockserver-state-infinispan/src/main/java/org/mockserver/state/infinispan/VersionedWrapper.java" >&2
echo "$JAVA_HOME"
echo "$OUT/cls:$OUT/libs/*"
