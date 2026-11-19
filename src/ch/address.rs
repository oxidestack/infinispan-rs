//! `org.infinispan.remoting.transport.Address` as the consistent hash sees it: a
//! `JGroupsAddress` wrapping a JGroups address, or a `PersistentUUID`.

use super::java_util::{HashMapInit, hash_map_iteration_order};
use jgroups::wire::address::Address as JAddress;

/// A node's address: a JGroups address or a persistent UUID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Address {
    /// `JGroupsAddress` (`remoting/transport/jgroups/JGroupsAddress.java`).
    JGroups(JAddress),
    /// `PersistentUUID` (`topology/PersistentUUID.java`): a `java.util.UUID` with value equality.
    Persistent {
        /// The high 64 bits.
        msb: i64,
        /// The low 64 bits.
        lsb: i64,
    },
}

impl Address {
    /// A JGroups UUID address from its two halves.
    pub fn uuid(msb: i64, lsb: i64) -> Self {
        Address::JGroups(JAddress::uuid(msb, lsb))
    }

    /// `(mostSigBits, leastSigBits)` for the UUID-shaped addresses `SyncConsistentHashFactory.nodeHash`
    /// feeds to MurmurHash3 (`jgroups UUID` and `ExtendedUUID` are `instanceof UUID`).
    pub fn uuid_bits(&self) -> Option<(i64, i64)> {
        match self {
            Address::JGroups(JAddress::Uuid { most_sig_bits, least_sig_bits })
            | Address::JGroups(JAddress::ExtendedUuid { most_sig_bits, least_sig_bits, .. }) => {
                Some((*most_sig_bits, *least_sig_bits))
            }
            Address::JGroups(JAddress::IpAddress { .. }) => None,
            Address::Persistent { msb, lsb } => Some((*msb, *lsb)),
        }
    }

    /// `Address.hashCode()`. `org.jgroups.util.UUID.hashCode` (`UUID.java:168`) and
    /// `java.util.UUID.hashCode` differ: the former mixes the halves with `>> 32` on both, the
    /// latter XORs them first.
    ///
    /// Panics for a bare `IpAddress`, which MockServer's clustering never uses as a member (the
    /// same convention `jgroups::wire::address::Address::compare_to` follows).
    pub fn hash_code(&self) -> i32 {
        match self {
            Address::JGroups(JAddress::Uuid { most_sig_bits: m, least_sig_bits: l })
            | Address::JGroups(JAddress::ExtendedUuid { most_sig_bits: m, least_sig_bits: l, .. }) => {
                ((m >> 32) ^ m ^ (l >> 32) ^ l) as i32
            }
            Address::JGroups(JAddress::IpAddress { .. }) => {
                panic!("hashCode of a bare IpAddress member is out of scope")
            }
            Address::Persistent { msb, lsb } => {
                let hilo = msb ^ lsb;
                ((hilo >> 32) as i32) ^ (hilo as i32)
            }
        }
    }
}

/// `Map<Address, Float>` with Java's `Map.equals`/`get` semantics over an insertion-ordered list.
#[derive(Debug, Clone, Default)]
pub struct CapacityFactors(pub Vec<(Address, f32)>);

impl CapacityFactors {
    /// A `HashMap` built with `init` by putting `entries` in order; entries are stored in the map's
    /// Java iteration order (see [`hash_map_iteration_order`]).
    pub fn java_hash_map(entries: Vec<(Address, f32)>, init: HashMapInit) -> Self {
        let hashes: Vec<i32> = entries.iter().map(|(a, _)| a.hash_code()).collect();
        let order = hash_map_iteration_order(&hashes, init);
        CapacityFactors(order.into_iter().map(|i| entries[i].clone()).collect())
    }

    /// `new HashMap<>(this)`: re-inserts in this map's iteration order into a copy-sized table.
    pub fn copied(&self) -> Self {
        Self::java_hash_map(self.0.clone(), HashMapInit::CopyOf(self.0.len()))
    }

    /// The capacity factor of `a`.
    pub fn get(&self, a: &Address) -> Option<f32> {
        self.0.iter().find(|(k, _)| k == a).map(|(_, v)| *v)
    }

    /// Sets the capacity factor of `a`, appending it if new.
    pub fn put(&mut self, a: Address, v: f32) {
        if let Some(e) = self.0.iter_mut().find(|(k, _)| *k == a) {
            e.1 = v;
        } else {
            self.0.push((a, v));
        }
    }

    /// The number of members.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl PartialEq for CapacityFactors {
    /// `AbstractMap.equals`: same size and every entry equal (`Float.equals`: bit-equal, NaN == NaN).
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self.0.iter().all(|(k, v)| other.get(k).is_some_and(|o| o.to_bits() == v.to_bits()))
    }
}
