#!/usr/bin/env bash
# Regenerates the topology and write-path vectors (tests/vectors/{murmur_hash3,sync_ch,...}.hex) by running the
# UNMODIFIED infinispan-core 14.0.35.Final + jgroups 5.2.28.Final jars (resolved through Maven from
# upstream-tools/topology-vectors/pom.xml) against the generator classes in
# upstream-tools/topology-vectors/src/main/java/gen/.
#
# Usage: upstream-tools/generate-topology-vectors.sh [--check]
#   --check writes into a scratch dir and cmp's against tests/vectors/, exit non-zero on any mismatch.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TV="$ROOT/upstream-tools/topology-vectors"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
OUT_DIR="$ROOT/tests/vectors"
CHECK=0
if [[ "${1:-}" == "--check" ]]; then CHECK=1; OUT_DIR="$WORK/out"; mkdir -p "$OUT_DIR"; fi

mvn -q -f "$TV/pom.xml" dependency:copy-dependencies -DoutputDirectory="$WORK/libs"
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"  # JDK 25 removed Subject.getSubject, which infinispan-core 14 calls
export PATH="$JAVA_HOME/bin:$PATH"
mkdir -p "$WORK/cls"
# MockServer's own classes, compiled unmodified from a MockServer checkout (read-only).
MS="${MOCKSERVER_SRC:-$ROOT/../../upstream/mockserver/mockserver}"  # the `mockserver` directory of a MockServer 8.0.0 checkout; the default is the layout of a consumer that has this crate under crates/
MSI="$MS/mockserver-state-infinispan/src/main/java/org/mockserver/state/infinispan"
MSC="$MS/mockserver-core/src/main/java/org/mockserver/state"
javac -cp "$WORK/libs/*" -d "$WORK/cls" "$TV"/src/main/java/gen/*.java \
  "$MSI/VersionedWrapper.java" "$MSI/InfinispanKeyValueStore.java" \
  "$MSC/KeyValueStore.java" "$MSC/InvalidationListener.java" "$MSC/Versioned.java" "$MSC/Blob.java"

FILES=()
run() { # <main class> <output file>
  java -Djava.util.logging.config.file=/dev/null -cp "$WORK/cls:$WORK/libs/*" "$1" "$OUT_DIR/$2" >/dev/null
  FILES+=("$2")
}
run gen.HashVectorGen murmur_hash3.hex
run gen.SyncChVectorGen sync_ch.hex
run gen.RchVectorGen rch.hex
run gen.TopologyWireVectorGen topology_wire.hex
run gen.WriteCommandVectorGen write_commands.hex
run gen.StateVectorGen state_transfer.hex
run gen.ExpiryVectorGen expiry.hex
# GEN-LIST-END

if [[ "$CHECK" == "1" ]]; then
  status=0
  for f in "${FILES[@]}"; do
    if cmp -s "$OUT_DIR/$f" "$ROOT/tests/vectors/$f"; then echo "PASS $f"; else echo "FAIL $f"; status=1; fi
  done
  exit $status
fi
echo "wrote: ${FILES[*]}"
