# infinispan-rs

<!-- languages:begin -->
[English](README.md) · [Русский](README.ru.md) · [中文](README.zh-CN.md) · **Español**
<!-- languages:end -->
<!-- translation of README.md: 4582ccd1a52a -->

Un port no oficial a Rust de las partes de [Infinispan](https://infinispan.org/) 14.0.35.Final que usa una caché
`REPL_SYNC` en clúster: el envoltorio de bytes de `GlobalMarshaller` y el códec de Java Object Serialization, el hashing
consistente (MurmurHash3, `SyncConsistentHashFactory`), la topología de la caché y el reequilibrado, la transferencia de
estado (state transfer), los comandos de escritura y su forma en la red, los listeners de la caché y la expiración, todo
ello sobre [jgroups](https://github.com/oxidestack/jgroups). Un nodo Rust construido con él se une a un clúster de nodos
Infinispan reales y habla su protocolo. Infinispan es una marca de sus propietarios; este proyecto no está afiliado al
proyecto Infinispan ni a Red Hat ni cuenta con su respaldo, y no contiene código fuente de Infinispan traducido
([NOTICE](NOTICE)).

Se escribió para [mock-server](https://github.com/oxidestack/mock-server), un MockServer en Rust que debe responder
exactamente como el original en Java, también cuando varias instancias comparten estado a través de Infinispan.

La versión portada es un hecho sobre el código, no la versión del crate: esta rama porta Infinispan 14.0.35.Final, y el
crate tiene su propio SemVer.

Estado: previo a la primera versión. El crate todavía no está publicado en crates.io; su primera versión será `0.1.0`.

## Instalación

Todavía no publicado. Cuando `0.1.0` esté en crates.io, la dependencia será la siguiente; el paquete es
`infinispan-rs` y el código lo nombra `infinispan`:

```toml
[dependencies]
infinispan-rs = "0.1"
```

Hasta entonces se compila desde el código fuente. Sus dependencias de los ports vecinos
[jgroups](https://github.com/oxidestack/jgroups) y [caffeine-rs](https://github.com/oxidestack/caffeine-rs) son
dependencias por ruta hasta que se publiquen, así que clónelos junto a este repositorio como `../jgroups-rs` y
`../caffeine-rs`. El crate necesita Rust 1.88 o posterior y funciona sobre [Tokio](https://tokio.rs).

## Ejemplo

Dos nodos de un clúster en memoria (la misma canalización, cada petición y respuesta serializadas, sin red) comparten
una caché replicada:

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

## Documentación

- Las guías, en inglés, ruso, chino y español: <https://oxidestack.github.io/infinispan-rs/>, con la
  [actualización](docs/i18n/es/guide/upgrading.md) entre versiones.
- La documentación de la API: en docs.rs cuando el crate se publique; hasta entonces, `cargo doc --open`. La página del
  crate dice qué clase de Infinispan porta cada módulo.
- Qué cambió en cada versión: [CHANGELOG.md](CHANGELOG.md).

## Compilar y probar

```sh
git submodule update --init   # the pinned upstream sources (upstream/, upstream-protostream/), read by some tests
cargo test                    # unit tests, tests over vectors recorded from the real Infinispan
make lint                     # clippy, warnings are errors
make deny                     # licences, advisories and sources (cargo install cargo-deny --locked)
make public-check             # the publication gate
make site                     # the documentation site
```

Los vectores de `tests/vectors/` están en el repositorio; regenerarlos requiere JDK 17 y Maven (véase
[tests/vectors/README.md](tests/vectors/README.md)), ejecutar las pruebas no. [CONTRIBUTING.md](CONTRIBUTING.md)
explica cómo contribuir; los commits llevan sign-off (DCO).

## Ayuda

Una pregunta sobre cómo funciona el crate puede ir primero a [DeepWiki](https://deepwiki.com/oxidestack/infinispan-rs):
responde a partir del código y la documentación de este repositorio, y donde no coincide con la documentación, la
documentación tiene razón. Las preguntas que no resuelva van a
[Discussions](https://github.com/oxidestack/infinispan-rs/discussions), y los errores a
[issues](https://github.com/oxidestack/infinispan-rs/issues).

## Licencia

Código: Apache-2.0 ([LICENSE](LICENSE)). Documentación: CC-BY-4.0 ([LICENSE-docs](LICENSE-docs)). Lo que porta el
crate figura en [NOTICE](NOTICE).
