#!/usr/bin/env bash
# Regenerates tests/vectors/*.hex from the pinned upstream sources/jars. See
# tests/vectors/README.md for what each file is and why it's trustworthy as a cross-check
# (real javac/java output against unmodified upstream classes, not hand-derived bytes).
#
# Usage:
#   upstream-tools/generate-vectors.sh [--check]
#
# Without --check: (re)writes tests/vectors/primitives.hex, marshall_size.hex, java_serial.hex.
# With --check: writes into a scratch dir instead and `cmp`s every file against what's committed
# under tests/vectors/, printing PASS/FAIL per file and exiting non-zero on any mismatch — this is
# what proves regeneration reproduces the committed vectors byte for byte.
#
# Dependencies fetched here (not vendored): net.jcip:jcip-annotations:1.0, resolved through the
# local Maven cache/repo1.maven.org. The Jackson jar is expected already present in the local
# Maven repo at the exact pinned version (jackson-databind 2.22.1, matching what MockServer's
# own build resolves) — this script does not silently substitute a different version.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAVA_VECTORS="$ROOT/upstream-tools/java-vectors"
UPSTREAM="$ROOT/upstream"
MS_UPSTREAM="${MOCKSERVER_SRC:-$ROOT/../../upstream/mockserver/mockserver}"  # the `mockserver` directory of a MockServer 8.0.0 checkout; the default is the layout of a consumer that has this crate under crates/
JACKSON_VERSION="2.22.1"
JACKSON_JAR="$(find "$HOME/.m2/repository/com/fasterxml/jackson/core/jackson-databind/$JACKSON_VERSION" -name '*.jar' 2>/dev/null | head -1)"

CHECK=0
if [[ "${1:-}" == "--check" ]]; then
  CHECK=1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

OUT_DIR="$ROOT/tests/vectors"
if [[ "$CHECK" == "1" ]]; then
  OUT_DIR="$WORK/out"
  mkdir -p "$OUT_DIR"
fi

if [[ -z "$JACKSON_JAR" ]]; then
  echo "FATAL: jackson-databind $JACKSON_VERSION not found under ~/.m2 — expected already resolved" >&2
  exit 1
fi

echo "== resolving net.jcip:jcip-annotations:1.0 =="
cat > "$WORK/pom.xml" <<'EOF'
<project><modelVersion>4.0.0</modelVersion>
<groupId>infinispan-rs</groupId><artifactId>vector-tools</artifactId><version>1</version>
<dependencies>
<dependency><groupId>net.jcip</groupId><artifactId>jcip-annotations</artifactId><version>1.0</version></dependency>
</dependencies>
</project>
EOF
mvn -q -f "$WORK/pom.xml" dependency:copy-dependencies -DoutputDirectory="$WORK/libs"
JCIP_JAR="$WORK/libs/jcip-annotations-1.0.jar"

echo "== primitives.hex (BytesObjectOutput/BytesObjectInput/Primitives) =="
mkdir -p "$WORK/out1"
javac -d "$WORK/out1" \
  -sourcepath "$JAVA_VECTORS/stubs:$UPSTREAM/commons/all/src/main/java" \
  "$UPSTREAM/core/src/main/java/org/infinispan/marshall/core/BytesObjectOutput.java" \
  "$UPSTREAM/core/src/main/java/org/infinispan/marshall/core/BytesObjectInput.java" \
  "$UPSTREAM/core/src/main/java/org/infinispan/marshall/core/AbstractBytesObjectInput.java" \
  "$UPSTREAM/core/src/main/java/org/infinispan/marshall/core/Primitives.java" \
  "$JAVA_VECTORS/gen/org/infinispan/marshall/core/VectorGen.java"
java -cp "$WORK/out1" org.infinispan.marshall.core.VectorGen "$OUT_DIR/primitives.hex"

echo "== marshall_size.hex (MarshallUtil.marshallSize) =="
mkdir -p "$WORK/out2"
javac -cp "$JCIP_JAR" -d "$WORK/out2" \
  -sourcepath "$JAVA_VECTORS/stubs:$UPSTREAM/commons/all/src/main/java" \
  "$UPSTREAM/commons/all/src/main/java/org/infinispan/commons/marshall/MarshallUtil.java" \
  "$JAVA_VECTORS/gen/org/infinispan/commons/marshall/MarshallSizeVectorGen.java"
java -cp "$WORK/out2:$JCIP_JAR" org.infinispan.commons.marshall.MarshallSizeVectorGen "$OUT_DIR/marshall_size.hex"

echo "== java_serial.hex (String/Long/Integer/VersionedWrapper/Blob/NodeSerialization/ExpectationEntry stand-in) =="
if [[ ! -d "$MS_UPSTREAM" ]]; then
  echo "FATAL: MockServer sources not found at $MS_UPSTREAM (needed to read VersionedWrapper.java/Blob.java, read-only)" >&2
  exit 1
fi
mkdir -p "$WORK/out3"
javac -encoding UTF-8 -cp "$JACKSON_JAR" -d "$WORK/out3" \
  "$JAVA_VECTORS/gen/javaserial/org/mockserver/state/ExpectationEntry.java" \
  "$JAVA_VECTORS/gen/javaserial/com/fasterxml/jackson/databind/node/NodeSerializationHelper.java" \
  "$MS_UPSTREAM/mockserver-core/src/main/java/org/mockserver/state/Blob.java" \
  "$MS_UPSTREAM/mockserver-state-infinispan/src/main/java/org/mockserver/state/infinispan/VersionedWrapper.java" \
  "$JAVA_VECTORS/gen/javaserial/JavaSerialVectorGen.java"
java -cp "$WORK/out3:$JACKSON_JAR" JavaSerialVectorGen "$OUT_DIR/java_serial.hex"

if [[ "$CHECK" == "1" ]]; then
  echo "== comparing regenerated vectors against committed tests/vectors/ =="
  status=0
  for f in primitives.hex marshall_size.hex java_serial.hex; do
    if cmp -s "$OUT_DIR/$f" "$ROOT/tests/vectors/$f"; then
      echo "PASS $f"
    else
      echo "FAIL $f (regenerated output differs from committed vector)"
      status=1
    fi
  done
  exit $status
fi

echo "== wrote tests/vectors/{primitives,marshall_size,java_serial}.hex =="
