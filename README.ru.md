# infinispan-rs

<!-- languages:begin -->
[English](README.md) · **Русский** · [中文](README.zh-CN.md) · [Español](README.es.md)
<!-- languages:end -->
<!-- translation of README.md: 4582ccd1a52a -->

Неофициальный порт на Rust тех частей [Infinispan](https://infinispan.org/) 14.0.35.Final, до которых доходит
кластерный кэш `REPL_SYNC`: байтовая оболочка `GlobalMarshaller` и кодек Java Object Serialization, консистентное
хеширование (MurmurHash3, `SyncConsistentHashFactory`), топология кэша и ребалансировка, передача состояния (state
transfer), команды записи и их форма на проводе, слушатели кэша и истечение срока — всё это работает поверх
[jgroups](https://github.com/oxidestack/jgroups). Узел на Rust, собранный из него, входит в кластер настоящих узлов
Infinispan и говорит на их протоколе. Infinispan — товарный знак его владельцев; этот проект не связан с проектом
Infinispan или Red Hat и не одобрен ими, и не содержит переведённого исходного кода Infinispan ([NOTICE](NOTICE)).

Он написан для [mock-server](https://github.com/oxidestack/mock-server) — MockServer на Rust, который должен отвечать в
точности как Java-оригинал, в том числе когда несколько экземпляров делят состояние через Infinispan.

Портированная версия — это свойство кода, а не версия крейта: эта ветка портирует Infinispan 14.0.35.Final, а у
крейта своя SemVer.

Статус: до первого выпуска. Крейт ещё не опубликован на crates.io; первым выпуском станет `0.1.0`.

## Установка

Ещё не опубликован. Когда `0.1.0` появится на crates.io, зависимость будет такой; пакет называется `infinispan-rs`, а
в коде — `infinispan`:

```toml
[dependencies]
infinispan-rs = "0.1"
```

До тех пор он собирается из исходников. Зависимости от соседних портов [jgroups](https://github.com/oxidestack/jgroups)
и [caffeine-rs](https://github.com/oxidestack/caffeine-rs) — зависимости по пути, пока те не опубликованы, поэтому
клонируйте их рядом с этим репозиторием как `../jgroups-rs` и `../caffeine-rs`. Крейту нужен Rust 1.88 или новее, он
работает на [Tokio](https://tokio.rs).

## Пример

Два узла кластера в памяти (тот же конвейер, каждый запрос и ответ маршалируется, без сети) разделяют
реплицируемый кэш:

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

## Документация

- Руководства на английском, русском, китайском и испанском: <https://oxidestack.github.io/infinispan-rs/>, включая
  [обновление](docs/i18n/ru/guide/upgrading.md) между выпусками.
- Документация API: на docs.rs, когда крейт будет опубликован; до тех пор — `cargo doc --open`. На странице крейта
  сказано, какой класс Infinispan портирует каждый модуль.
- Что изменилось в каждом выпуске: [CHANGELOG.md](CHANGELOG.md).

## Сборка и тесты

```sh
git submodule update --init   # the pinned upstream sources (upstream/, upstream-protostream/), read by some tests
cargo test                    # unit tests, tests over vectors recorded from the real Infinispan
make lint                     # clippy, warnings are errors
make deny                     # licences, advisories and sources (cargo install cargo-deny --locked)
make public-check             # the publication gate
make site                     # the documentation site
```

Векторы в `tests/vectors/` закоммичены; для их пересоздания нужны JDK 17 и Maven (см.
[tests/vectors/README.md](tests/vectors/README.md)), для запуска тестов — нет. Как внести вклад, описано в
[CONTRIBUTING.md](CONTRIBUTING.md); коммиты подписываются (DCO).

## Помощь

Вопрос о том, как работает крейт, можно сначала задать [DeepWiki](https://deepwiki.com/oxidestack/infinispan-rs): он
отвечает по коду и документации этого репозитория, а где он расходится с документацией, права документация. Вопросы,
которые он не решил, — в [Discussions](https://github.com/oxidestack/infinispan-rs/discussions), ошибки — в
[issues](https://github.com/oxidestack/infinispan-rs/issues).

## Лицензия

Код: Apache-2.0 ([LICENSE](LICENSE)). Документация: CC-BY-4.0 ([LICENSE-docs](LICENSE-docs)). Что портирует крейт,
указано в [NOTICE](NOTICE).
