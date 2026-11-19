# infinispan-rs

<!-- languages:begin -->
[English](README.md) · [Русский](README.ru.md) · **中文** · [Español](README.es.md)
<!-- languages:end -->
<!-- translation of README.md: 4582ccd1a52a -->

[Infinispan](https://infinispan.org/) 14.0.35.Final 中集群 `REPL_SYNC` 缓存所用部分的非官方 Rust 移植：`GlobalMarshaller`
字节封装和 Java Object Serialization 编解码、一致性哈希（MurmurHash3、`SyncConsistentHashFactory`）、缓存拓扑与再平衡、
状态传输（state transfer）、写命令及其线路格式、缓存监听器和过期，全部运行在 [jgroups](https://github.com/oxidestack/jgroups)
之上。用它构建的 Rust 节点可以加入由真实 Infinispan 节点组成的集群，并使用它们的线路协议通信。Infinispan 是其所有者的商标；
本项目与 Infinispan 项目或 Red Hat 没有关联，也未获得其认可，并且不包含翻译过来的 Infinispan 源代码（[NOTICE](NOTICE)）。

它是为 [mock-server](https://github.com/oxidestack/mock-server) 编写的。mock-server 是一个必须与 Java 原版作出完全相同应答的
Rust 版 MockServer，包括多个实例通过 Infinispan 共享状态的情况。

所移植的版本是代码的属性，而不是 crate 的版本：此分支移植的是 Infinispan 14.0.35.Final，而 crate 有自己的 SemVer 版本号。

状态：尚未发布。该 crate 还没有在 crates.io 上发布；首个版本将是 `0.1.0`。

## 安装

尚未发布。`0.1.0` 发布到 crates.io 之后，依赖将如下所示；包名为 `infinispan-rs`，在代码中使用 `infinispan`：

```toml
[dependencies]
infinispan-rs = "0.1"
```

在此之前需要从源码构建。在 [jgroups](https://github.com/oxidestack/jgroups) 和
[caffeine-rs](https://github.com/oxidestack/caffeine-rs) 这两个相邻移植发布之前，对它们的依赖是路径依赖，因此请把它们检出到本仓库旁边，
目录名分别为 `../jgroups-rs` 和 `../caffeine-rs`。该 crate 需要 Rust 1.88 或更高版本，运行在 [Tokio](https://tokio.rs) 之上。

## 示例

一个内存集群中的两个节点（相同的处理管线，每个请求和应答都经过编组，没有网络）共享一个复制缓存：

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

## 文档

- 指南（英文、俄文、中文和西班牙文）：<https://oxidestack.github.io/infinispan-rs/>，包括版本之间的
  [升级](docs/i18n/zh/guide/upgrading.md)。
- API 文档：crate 发布后位于 docs.rs；在此之前请使用 `cargo doc --open`。crate 首页说明了每个模块移植的是哪个 Infinispan 类。
- 每个版本的变更：[CHANGELOG.md](CHANGELOG.md)。

## 构建与测试

```sh
git submodule update --init   # the pinned upstream sources (upstream/, upstream-protostream/), read by some tests
cargo test                    # unit tests, tests over vectors recorded from the real Infinispan
make lint                     # clippy, warnings are errors
make deny                     # licences, advisories and sources (cargo install cargo-deny --locked)
make public-check             # the publication gate
make site                     # the documentation site
```

`tests/vectors/` 中的测试向量已提交；重新生成它们需要 JDK 17 和 Maven（见 [tests/vectors/README.md](tests/vectors/README.md)），
运行测试则不需要。如何参与贡献见 [CONTRIBUTING.md](CONTRIBUTING.md)；提交需要签署（DCO）。

## 获取帮助

关于本 crate 如何工作的问题，可以先问 [DeepWiki](https://deepwiki.com/oxidestack/infinispan-rs)：它根据本仓库的代码和文档作答，
当它与文档不一致时，以文档为准。它解决不了的问题请到 [Discussions](https://github.com/oxidestack/infinispan-rs/discussions)，
缺陷请提交到 [issues](https://github.com/oxidestack/infinispan-rs/issues)。

## 许可证

代码：Apache-2.0（[LICENSE](LICENSE)）。文档：CC-BY-4.0（[LICENSE-docs](LICENSE-docs)）。本 crate 移植了哪些内容，见
[NOTICE](NOTICE)。
