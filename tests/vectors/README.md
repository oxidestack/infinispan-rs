# Test vectors

Real, upstream-produced byte vectors, not hand-derived. Each `.hex` file is
`<label>\t<lowercase-hex-bytes>` per line, one vector per line.

- `primitives.hex` — `BytesObjectOutput`/`Primitives` wire format (Infinispan 14.0.35.Final,
  `core/src/main/java/org/infinispan/marshall/core/{BytesObjectOutput,BytesObjectInput,
  AbstractBytesObjectInput,Primitives}.java`, compiled and run unmodified).
- `marshall_size.hex` — the 6-bit-first varint (`commons/all/.../marshall/MarshallUtil.java`
  `marshallSize`), compiled and run unmodified.
- `java_serial.hex` — JDK Object Serialization Stream Protocol vectors for `java.lang.String`,
  boxed `Long`/`Integer`, and three of the six MockServer "user value" classes the
  clustered state store marshals: `VersionedWrapper` and `Blob` (both the real, unmodified classes read from a
  MockServer checkout and compiled standalone — both are dependency-free) and Jackson
  `NodeSerialization` (the real, unmodified class from the pinned `jackson-databind-2.22.1.jar` in
  the local Maven repo, constructed via a same-package helper since the class itself is package-
  private). `ExpectationEntry` is a **structural stand-in**, not the real class: same package,
  class name, `serialVersionUID`, field set/order and custom `writeObject`/`readObject` pattern
  (`defaultWriteObject()` + `writeUTF(json)`), but without the Jackson/`ExpectationDTO`/
  `Expectation` dependency graph the real class needs to produce its JSON payload — see the doc
  comment on `gen/javaserial/org/mockserver/state/ExpectationEntry.java`. The wire protocol
  (class descriptor bytes, handle numbering, block-data framing) this produces is the same as the
  real class would produce for equivalent field values; only the JSON *content* differs, and that
  content is opaque payload bytes to the protocol either way.

## Topology vectors (consistent hash, topology)

Produced by `upstream-tools/generate-topology-vectors.sh`, which resolves **infinispan-core 14.0.35.Final**
and jgroups 5.2.28.Final through Maven (`upstream-tools/topology-vectors/pom.xml`) and runs the generator
classes in `upstream-tools/topology-vectors/src/main/java/gen/` against the unmodified jars. It needs
JDK 17 (JDK 25 removed `Subject.getSubject`, which infinispan-core 14 still calls when a cache manager
starts, so the script pins `java_home -v 17`). Where a vector is a serialised object, the bytes come from
the real `GlobalMarshaller` of a started (non-clustered) `DefaultCacheManager`, not from a stub.

- `murmur_hash3.hex` — `MurmurHash3` x64 128/64/32 over `byte[]`/`long[]`, `hash(int)`,
  `hash(String)` (UTF-16 code units, incl. lone surrogates) and `HashFunctionPartitioner.getSegment`
  with 256 and 7 segments.
- `sync_ch.hex` — `SyncConsistentHashFactory.create` (256/60/7 segments, 1-3 owners, 1-8 members,
  capacity-factor variants), `updateMembers` and `rebalance`; each line is the per-segment owner indices.
- `rch.hex` — `SyncReplicatedConsistentHashFactory` results (create, update, union) serialised by the
  real marshaller, including `InstanceReusingAdvancedExternalizer` back-references and the iteration
  order of the `HashMap` capacity-factor copy.
- `topology_wire.hex` — `CacheTopology` in every phase, `CacheJoinInfo`, the topology commands
  (`CacheJoin`, `CacheLeave`, `RebalancePhaseConfirm`, `RebalanceStatusRequest`, `RebalanceStart`,
  `TopologyUpdate`, `CacheStatusRequest`, `TopologyUpdateStable`, `HeartBeat`) and responses
  (`SuccessfulResponse` of status/manager/enum/boolean values, `CacheNotFoundResponse`,
  `ExceptionResponse`), as the real marshaller writes them. The scenario objects are defined twice, in
  `gen/Scenarios.java` and `tests/common/scenarios.rs`.

`upstream-tools/generate-topology-vectors.sh --check` regenerates into a scratch directory and `cmp`s
against the committed files.

## Reproducing

The generator sources (and the compile-only stub leaves needed to compile
`BytesObjectOutput`/`BytesObjectInput`/`MarshallUtil` without pulling in Infinispan's full DI
graph — see the doc comment on each stub file) live in `upstream-tools/java-vectors/`, driven by
`upstream-tools/generate-vectors.sh`:

```bash
# from the repository root; MOCKSERVER_SRC is the `mockserver` directory of a MockServer 8.0.0 checkout
export MOCKSERVER_SRC=...

# Regenerate tests/vectors/*.hex in place:
upstream-tools/generate-vectors.sh

# Or verify regeneration reproduces the committed vectors byte for byte, without touching them
# (writes to a scratch dir and `cmp`s against tests/vectors/; exits non-zero on any mismatch):
upstream-tools/generate-vectors.sh --check
```

The script fetches `net.jcip:jcip-annotations:1.0` via Maven (needed to compile the real
`MarshallUtil.java`) and expects `jackson-databind-2.22.1.jar` already resolved in the local Maven
repository (the version MockServer's own build pins). It reads `VersionedWrapper.java` and
`Blob.java` from the MockServer checkout `MOCKSERVER_SRC` names, read-only — nothing there is
written to.

## Write-path vectors (REPL_SYNC write path)

- `write_commands.hex` — produced by `upstream-tools/generate-topology-vectors.sh` (generator
  `gen/WriteCommandVectorGen.java`). The commands are **built by the real `CacheImpl`/`EncoderCache`** on a
  local cache with `JavaSerializationMarshaller` (as MockServer's clustered manager), driven through
  MockServer's own `InfinispanKeyValueStore` and `VersionedWrapper` (compiled unmodified from the pinned
  `upstream/mockserver` submodule), captured by an interceptor placed after `VersionInterceptor`, and
  marshalled by the real `GlobalMarshaller` as the `SingleRpcCommand` that `RpcManagerImpl` sends.
  The invocation id's address is replaced by a cluster-shaped one (a local cache stamps a
  `LocalModeAddress`, which cannot be marshalled). Labels: `put:*`, `putIfAbsent:*`, `compute:*` (the real
  `SerializedLambda` of `InfinispanKeyValueStore.put`, name `lambda$put$ecf05865$1`), `replace:cas`
  (carries the `NumericVersion` `VersionInterceptor` adds), `remove:*`, `clear`, `clusteredget`, and the
  responses `resp:*` (successful/unsuccessful with a value, null, booleans, `InternalCacheValue`s,
  `TimeoutException`, `OutdatedTopologyException`).

## State-transfer vectors (state transfer)

`state_transfer.hex`, from `upstream-tools/topology-vectors/src/main/java/gen/StateVectorGen.java` (run by
`upstream-tools/generate-topology-vectors.sh`, `--check` verifies it): `StateTransferStartCommand` /
`StateTransferCancelCommand` with the `IntSet` classes `IntSetsExternalization` writes (`SmallIntSet` for 256, 3
and 1 segments, `SingletonIntSet`, `EmptyIntSet`), `StateResponseCommand` with `StateChunk`s of every
`InternalCacheEntry` class a REPL cache holds (immortal, mortal, transient, transient-mortal and the four metadata
kinds), an empty last chunk (`Collections.emptyList()`), several segments in one command (the iteration order of
Java's `HashMap<Integer, StateChunk>`), and a push transfer. The `probe:` lines come from a real two-node
REPL_SYNC cluster (TCP stack on loopback): the class of the entry on the provider and in the joiner's data
container after state transfer, for puts with no expiry, a version, a lifespan, a max idle and both; they are
identical on both nodes.

## Expiration vectors (expiration)

`expiry.hex`, from `upstream-tools/topology-vectors/src/main/java/gen/ExpiryVectorGen.java` (run by
`upstream-tools/generate-topology-vectors.sh`, `--check` verifies it): the `RemoveExpiredCommand` (id 58) a node
sends the primary owner when a read finds an in-memory entry expired, **built by the real `CacheImpl`**
(`removeLifespanExpired`, `removeMaxIdleExpired`, with and without the `ZERO_LOCK_ACQUISITION_TIMEOUT` flag a read
adds), captured by an interceptor before `CallInterceptor` and marshalled by the real `GlobalMarshaller` as the
`SingleRpcCommand` `RpcManagerImpl` sends. The invocation id's address and the segment are replaced as in the
write command vectors. Labels: `expire:lifespan-string`, `expire:lifespan-versioned` (a `VersionedWrapper` value),
`expire:lifespan-plain-flags` (no zero-lock bit), `expire:maxidle` (no lifespan, `maxIdle` true), each followed by a
`flags:<label>` line holding the command's flag bit set in hex, which pins the Rust flag constants
(`SKIP_CACHE_LOAD` bit 7, `SKIP_XSITE_BACKUP` bit 17) to the real ones.

## Mixed-cluster vectors (mixed cluster)

`mixed_cluster_compute_lambda.hex`: the `Cache.compute` lambda (the `LambdaMarshaller` bytes from the lambda tag byte
to the end of `instantiatedMethodType`, `InfinispanKeyValueStore.put`'s `lambda$put$ecf05865$1`) as it crossed the
JGroups TCP wire in a cluster of `mockserver/mockserver:clustered-8.0.0` and our node, reassembled from the pcaps
that `conformance/cluster/tap.py` records (`conformance/cluster/pcap_extract.py --dump`). `captured-java` was sent by
the Java node (received by ours), `captured-rs` by our node (replayed by the Java backup); both are identical to the
lambda in `write_commands.hex` `compute:store-put-new`.
