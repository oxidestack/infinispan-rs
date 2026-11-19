//! Port of `org.infinispan.commons.configuration.ClassAllowList`
//! (`commons/all/.../configuration/ClassAllowList.java`, 14.0.35.Final) and the gate
//! `org.infinispan.commons.marshall.CheckedInputStream.resolveClass` applies during
//! deserialization (`commons/all/.../marshall/CheckedInputStream.java:22-31`).
//!
//! `ClassAllowList.isSafeClass`: an exact-name match against a fast set first (which upstream
//! memoizes future regex hits into — see the `remember` parameter below), then a `find()` (not
//! `matches()`) scan of the regex list, in order.

use regex::Regex;

/// `ClassAllowList.SYS_ALLOWED_CLASSES` (`:53-97`): exact class names always allowed, independent
/// of any constructor-supplied list. `KeyValueWithPrevious` (an Infinispan-internal class for
/// client listeners/jcache-remote) is omitted — it is meaningless outside a running Infinispan
/// process and no recorded vector needs it.
const SYS_ALLOWED_CLASSES: &[&str] = &[
    "[B", "[S", "[I", "[J", "[F", "[D", "[C", "[Z", // primitive arrays
    "java.lang.Byte",
    "java.lang.Short",
    "java.lang.Integer",
    "java.lang.Long",
    "java.lang.Float",
    "java.lang.Double",
    "java.lang.Character",
    "java.lang.String",
    "java.lang.Boolean",
    "java.math.BigInteger",
    "java.math.BigDecimal",
    "java.time.Instant",
    "java.time.Ser",
    "java.util.Date",
    "java.lang.Enum",
    "java.lang.Number",
];

/// `ClassAllowList.SYS_ALLOWED_REGEXP` (`:93`): reference-array shape, e.g. `[Ljava.lang.Byte;`.
const SYS_ALLOWED_REGEXP: &[&str] = &[r"^\[[\[L].*\;$"];

/// `ClassAllowList` (`:39-179`).
pub struct ClassAllowList {
    classes: std::collections::HashSet<String>,
    patterns: Vec<Regex>,
}

impl ClassAllowList {
    /// `new ClassAllowList(classes, regexps, classLoader)` (`:133-140`) — the constructor
    /// `DeserializationAllowListTest` and `InfinispanStateBackend` both use, with an explicit
    /// `classes`/`regexps` pair on top of the always-on `SYS_ALLOWED_*` sets.
    pub fn new<I, J>(classes: I, regexps: J) -> Self
    where
        I: IntoIterator<Item = String>,
        J: IntoIterator<Item = String>,
    {
        let mut set: std::collections::HashSet<String> =
            SYS_ALLOWED_CLASSES.iter().map(|s| s.to_string()).collect();
        set.extend(classes);

        let mut patterns: Vec<Regex> = SYS_ALLOWED_REGEXP
            .iter()
            .map(|p| Regex::new(p).expect("SYS_ALLOWED_REGEXP patterns are known-valid"))
            .collect();
        for p in regexps {
            patterns.push(Regex::new(&p).unwrap_or_else(|e| panic!("invalid regexp {p}: {e}")));
        }

        Self {
            classes: set,
            patterns,
        }
    }

    /// An allow list that allows no class.
    pub fn empty() -> Self {
        Self::new(std::iter::empty(), std::iter::empty())
    }

    /// `ClassAllowList.isSafeClass` (`:142-155`): exact-name set lookup first, then `find()`
    /// (substring search, not a full-string `matches()`) over the regex list in order. Upstream
    /// memoizes a regex hit into the classes set "to avoid future regex checks"; that is a pure
    /// performance detail with no wire-format or decision effect, so this port skips it (`&self`,
    /// not `&mut self`).
    pub fn is_safe_class(&self, class_name: &str) -> bool {
        if self.classes.contains(class_name) {
            return true;
        }
        self.patterns.iter().any(|p| p.is_match(class_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The real regex list from `InfinispanStateBackend.CLUSTERED_ALLOW_LIST_PATTERNS`
    /// (MockServer's `mockserver-state-infinispan/.../InfinispanStateBackend.java:74-85`).
    fn clustered_allow_list() -> ClassAllowList {
        ClassAllowList::new(
            std::iter::empty(),
            [
                r"org\.mockserver\.state\.infinispan\..*",
                r"org\.mockserver\.state\..*",
                r"org\.mockserver\.mock\..*",
                r"org\.mockserver\.model\..*",
                r"org\.mockserver\.matchers\..*",
                r"com\.fasterxml\.jackson\..*",
                r"java\.lang\..*",
                r"java\.util\..*",
                r"java\.time\..*",
                r"\[B",
            ]
            .map(String::from),
        )
    }

    // --- ported from DeserializationAllowListTest.AllowListEnforcementTest (18 @Test total;
    //     the other 3 — ExpectationEntrySerializationTest's round-trip cases — are ported in
    //     java_serial.rs's tests, since they exercise the codec, not the allow-list). ---

    #[test]
    fn should_allow_versioned_wrapper() {
        assert!(clustered_allow_list()
            .is_safe_class("org.mockserver.state.infinispan.VersionedWrapper"));
    }

    #[test]
    fn should_allow_expectation_entry() {
        assert!(clustered_allow_list().is_safe_class("org.mockserver.state.ExpectationEntry"));
    }

    #[test]
    fn should_allow_expectation() {
        assert!(clustered_allow_list().is_safe_class("org.mockserver.mock.Expectation"));
    }

    #[test]
    fn should_allow_http_request() {
        assert!(clustered_allow_list().is_safe_class("org.mockserver.model.HttpRequest"));
    }

    #[test]
    fn should_allow_jackson_object_node() {
        assert!(
            clustered_allow_list().is_safe_class("com.fasterxml.jackson.databind.node.ObjectNode")
        );
    }

    #[test]
    fn should_allow_java_lang_string() {
        assert!(clustered_allow_list().is_safe_class("java.lang.String"));
    }

    #[test]
    fn should_allow_java_util_hash_map() {
        assert!(clustered_allow_list().is_safe_class("java.util.HashMap"));
    }

    #[test]
    fn should_allow_byte_array() {
        assert!(clustered_allow_list().is_safe_class("[B"));
    }

    #[test]
    fn should_reject_apache_commons_collections() {
        assert!(!clustered_allow_list()
            .is_safe_class("org.apache.commons.collections4.functors.InvokerTransformer"));
    }

    #[test]
    fn should_reject_spring_framework_types() {
        assert!(!clustered_allow_list()
            .is_safe_class("org.springframework.beans.factory.config.MethodInvokingBean"));
    }

    #[test]
    fn should_reject_random_unknown_gadget_class() {
        assert!(!clustered_allow_list().is_safe_class("com.evil.GadgetChainEntry"));
    }

    #[test]
    fn should_reject_javax_naming_context() {
        assert!(!clustered_allow_list().is_safe_class("javax.naming.InitialContext"));
    }

    #[test]
    fn should_reject_java_rmi_types() {
        assert!(!clustered_allow_list().is_safe_class("java.rmi.server.RemoteObject"));
    }

    #[test]
    fn should_allow_and_reject_via_marshal_round_trip() {
        // shouldMarshalAndUnmarshalAllowedType / shouldRejectDisallowedTypeDuringDeserialization:
        // ported as full java_serial round trips gated by this allow-list, in java_serial.rs
        // (`allow_list_gates_class_descriptor_resolution`), since they need the codec.
    }
}
