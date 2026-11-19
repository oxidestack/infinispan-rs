# infinispan-rs

<!-- languages:begin -->
**English** · [Русский](README.ru.md) · [中文](README.zh-CN.md) · [Español](README.es.md)
<!-- languages:end -->

An unofficial Rust port of the parts of [Infinispan](https://infinispan.org/) 14.0.35.Final that a clustered
`REPL_SYNC` cache reaches: the `GlobalMarshaller` byte envelope and Java Object Serialization codec, consistent hashing
(MurmurHash3, `SyncConsistentHashFactory`), cache topology and rebalance, state transfer, the write commands and their
wire form, cache listeners and expiration, all running over [jgroups](https://github.com/oxidestack/jgroups). A Rust
node built from it joins a cluster of real Infinispan nodes and speaks their wire protocol. Infinispan is a trademark
of its owners; this project is not affiliated with or endorsed by the Infinispan project or Red Hat, and carries no
translated Infinispan source ([NOTICE](NOTICE)).

It was written for [mock-server](https://github.com/oxidestack/mock-server), a Rust MockServer that must answer
exactly as the Java original does, including when several instances share state through Infinispan.

The ported version is a fact about the code, not the crate's version: this branch ports Infinispan 14.0.35.Final, and
the crate has its own SemVer.

Status: pre-release. The crate is not published on crates.io yet; its first release will be `0.1.0`.

## Install

Not published yet. Once `0.1.0` is on crates.io, the dependency will be the following; the package is `infinispan-rs`
and the code names it `infinispan`:

```toml
[dependencies]
infinispan-rs = "0.1"
```

Until then it is built from source. Its dependencies on the sibling ports
[jgroups](https://github.com/oxidestack/jgroups) and [caffeine-rs](https://github.com/oxidestack/caffeine-rs) are path
dependencies until those are published, so check them out next to this repository as `../jgroups-rs` and
`../caffeine-rs`. The crate needs Rust 1.88 or later and runs on [Tokio](https://tokio.rs).

## Example

Two nodes of an in-memory cluster (the same pipeline, every request and answer marshalled, no network) share a
replicated cache:

```rust
use infinispan::cache::testing::ReplCluster;
use infinispan::commands::object::CacheObject;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let cluster = ReplCluster::start(2, &["demo"]).await;
    let key = CacheObject::string("greeting");
    let value = CacheObject::string("hello");

    cluster.cache(0, "demo").put(&key, &value).await.unwrap();
    assert_eq!(cluster.cache(1, "demo").get(&key).await.unwrap(), Some(value));
}
```

## Documentation

- The guides, in English, Russian, Chinese and Spanish: <https://oxidestack.github.io/infinispan-rs/>, with
  [upgrading](docs/guide/upgrading.md) between releases.
- The API documentation: on docs.rs once the crate is published; until then, `cargo doc --open`. The crate-level
  page says which Infinispan class each module ports.
- What changed in each release: [CHANGELOG.md](CHANGELOG.md).

## Build and test

```sh
git submodule update --init   # the pinned upstream sources (upstream/, upstream-protostream/), read by some tests
cargo test                    # unit tests, tests over vectors recorded from the real Infinispan
make lint                     # clippy, warnings are errors
make deny                     # licences, advisories and sources (cargo install cargo-deny --locked)
make public-check             # the publication gate
make site                     # the documentation site
```

The vectors in `tests/vectors/` are committed; regenerating them needs JDK 17 and Maven (see
[tests/vectors/README.md](tests/vectors/README.md)), running the tests does not. [CONTRIBUTING.md](CONTRIBUTING.md)
says how to contribute; commits are signed off (DCO).

## Getting help

A question about how the crate works can go to [DeepWiki](https://deepwiki.com/oxidestack/infinispan-rs) first: it
answers from this repository's code and documentation, and where it disagrees with the documentation, the
documentation is right. Questions it does not settle go to
[Discussions](https://github.com/oxidestack/infinispan-rs/discussions), bugs to
[issues](https://github.com/oxidestack/infinispan-rs/issues).

## Licence

Code: Apache-2.0 ([LICENSE](LICENSE)). Documentation: CC-BY-4.0 ([LICENSE-docs](LICENSE-docs)). What the crate ports
is in [NOTICE](NOTICE).
