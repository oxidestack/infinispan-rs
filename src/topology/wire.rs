//! The `ID_INTERNAL` object-graph subset the topology classes ride on, over
//! [`crate::marshall`]'s byte codecs: `GlobalMarshaller.writeInternal`
//! (`core/.../marshall/core/GlobalMarshaller.java`), `InstanceReusingAdvancedExternalizer`'s
//! back-reference framing (`commons/.../marshall/InstanceReusingAdvancedExternalizer.java`) and the
//! collection/map/address externalizers topology objects use. Each externalizer id is from
//! `commons/.../marshall/Ids.java`. Verified against bytes produced by the real marshaller
//! (`tests/vectors/topology_wire.hex`).

use crate::ch::address::{Address, CapacityFactors};
use crate::ch::java_util::HashMapInit;
use crate::ch::replicated_consistent_hash::ReplicatedConsistentHash;
use crate::topology::cache_topology::CacheTopology;
use crate::marshall::error::{Error, Result};
use crate::marshall::global::{self, Value};
use crate::marshall::marshall_util::{marshall_size, unmarshall_size};
use crate::marshall::primitives::{ByteReader, ByteWriter, PrimitiveValue};
use jgroups::wire::address::{read_address, write_address};
use jgroups::wire::stream::WireReader;

/// `GlobalMarshaller.ID_NULL`: a `null`.
pub const ID_NULL: u8 = 0x00;
/// `GlobalMarshaller.ID_PRIMITIVE`: a primitive, string or primitive array.
pub const ID_PRIMITIVE: u8 = 0x01;
/// `GlobalMarshaller.ID_INTERNAL`: an object with an internal externalizer, whose id follows.
pub const ID_INTERNAL: u8 = 0x02;

// Ids.java
/// `Ids.MAPS`: the externalizer of maps.
pub const ID_MAPS: u8 = 1;
/// `Ids.COLLECTIONS`: the externalizer of collections.
pub const ID_COLLECTIONS: u8 = 2;
/// `Ids.JGROUPS_ADDRESS`: a JGroups address.
pub const ID_JGROUPS_ADDRESS: u8 = 20;
/// `Ids.REPLICATED_CONSISTENT_HASH`.
pub const ID_REPLICATED_CONSISTENT_HASH: u8 = 28;
/// `Ids.PERSISTENT_UUID`.
pub const ID_PERSISTENT_UUID: u8 = 97;
/// `Ids.IMMUTABLE_MAP`.
pub const ID_IMMUTABLE_MAP: u8 = 105;
/// `Ids.IMMUTABLE_LIST_COPY`.
pub const ID_IMMUTABLE_LIST_COPY: u8 = 147;

// CollectionExternalizer kinds
const ARRAY_LIST: u8 = 0;
const SINGLETON_LIST: u8 = 2;
const EMPTY_LIST: u8 = 3;
// MapExternalizer kinds
const HASHMAP: u8 = 0;
const SINGLETONMAP: u8 = 6;
const EMPTYMAP: u8 = 7;

// InstanceReusingAdvancedExternalizer
const ID_NO_REPEAT: u8 = 0x01;
const ID_REPEAT_OBJECT_NEAR: u8 = 0x02;
const ID_REPEAT_OBJECT_NEARISH: u8 = 0x03;
const ID_REPEAT_OBJECT_FAR: u8 = 0x04;

/// Objects whose externalizer extends `InstanceReusingAdvancedExternalizer`: a second occurrence
/// within one marshalling session is written as a back-reference.
#[derive(Debug, Clone, PartialEq)]
pub enum Reusable {
    /// A node address.
    Address(Address),
    /// A replicated consistent hash.
    Rch(ReplicatedConsistentHash),
    /// `CacheTopology.equals` (topologyId, rebalanceId, phase, CHs, actualMembers).
    Topology(Box<CacheTopology>),
    /// An object with identity equality (`ManagerStatusResponse` defines no `equals`): it takes a
    /// slot in the back-reference index but can never be referenced again.
    Identity(usize),
}

/// A `BytesObjectOutput` plus the thread-local `ReusableData` of the outermost reusing externalizer.
pub struct ObjectOutput {
    /// The bytes written so far.
    pub w: ByteWriter,
    reuse: Option<Vec<Reusable>>,
    identity_counter: usize,
}

impl Default for ObjectOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectOutput {
    /// An empty output, outside any reusing session.
    pub fn new() -> Self {
        Self { w: ByteWriter::new(), reuse: None, identity_counter: 0 }
    }

    /// The bytes written.
    pub fn into_bytes(self) -> Vec<u8> {
        self.w.into_bytes()
    }

    /// A fresh [`Reusable::Identity`] key.
    pub(crate) fn next_identity(&mut self) -> Reusable {
        self.identity_counter += 1;
        Reusable::Identity(self.identity_counter)
    }

    /// `writeObject(null)`.
    pub fn write_null(&mut self) {
        self.w.write_unsigned_byte(ID_NULL);
    }

    pub(crate) fn write_internal_header(&mut self, id: u8) {
        self.w.write_unsigned_byte(ID_INTERNAL);
        self.w.write_unsigned_byte(id);
    }

    /// `InstanceReusingAdvancedExternalizer.writeObject`.
    pub(crate) fn write_reusable(&mut self, obj: Reusable, has_children: bool, body: impl FnOnce(&mut Self)) {
        let outermost = has_children && self.reuse.is_none();
        if outermost {
            self.reuse = Some(Vec::new());
        }
        let found = self.reuse.as_ref().and_then(|d| d.iter().position(|x| *x == obj).map(|p| (p, d.len())));
        if let Some((id, offset)) = found {
            let diff = id as i64 - offset as i64;
            if diff >= -256 {
                self.w.write_unsigned_byte(ID_REPEAT_OBJECT_NEAR);
                self.w.write_unsigned_byte(diff as u8);
            } else if diff >= -65536 {
                self.w.write_unsigned_byte(ID_REPEAT_OBJECT_NEARISH);
                self.w.write_short(diff as i16);
            } else {
                self.w.write_unsigned_byte(ID_REPEAT_OBJECT_FAR);
                self.w.write_int(id as i32);
            }
        } else {
            self.w.write_unsigned_byte(ID_NO_REPEAT);
            body(self);
            if let Some(d) = &mut self.reuse {
                d.push(obj);
            }
        }
        if outermost {
            self.reuse = None;
        }
    }

    /// `writeObject(address)`: `JGroupsAddress` (id 20, `hasChildren=false`) or `PersistentUUID` (97).
    pub fn write_address(&mut self, a: &Address) {
        match a {
            Address::JGroups(j) => {
                self.write_internal_header(ID_JGROUPS_ADDRESS);
                let j = j.clone();
                self.write_reusable(Reusable::Address(a.clone()), false, move |o| {
                    let mut buf = Vec::new();
                    write_address(Some(&j), &mut buf);
                    o.w.write_raw(&buf);
                });
            }
            Address::Persistent { msb, lsb } => {
                self.write_internal_header(ID_PERSISTENT_UUID);
                self.w.write_long(*msb);
                self.w.write_long(*lsb);
            }
        }
    }

    /// `writeObject(Immutables.immutableListCopy(addresses))`: `emptyList`, `singletonList` or
    /// `ImmutableListCopy` by size (`Immutables.java:64-70`).
    pub fn write_immutable_address_list(&mut self, list: &[Address]) {
        match list.len() {
            0 => {
                self.write_internal_header(ID_COLLECTIONS);
                self.w.write_unsigned_byte(EMPTY_LIST);
            }
            1 => {
                self.write_internal_header(ID_COLLECTIONS);
                self.w.write_unsigned_byte(SINGLETON_LIST);
                self.write_address(&list[0]);
            }
            n => {
                self.write_internal_header(ID_IMMUTABLE_LIST_COPY);
                marshall_size(&mut self.w, n as i32);
                for a in list {
                    self.write_address(a);
                }
            }
        }
    }

    /// `writeObject(ArrayList<Address>)`: `CollectionExternalizer.ARRAY_LIST`.
    pub fn write_array_list_of_addresses(&mut self, list: &[Address]) {
        self.write_internal_header(ID_COLLECTIONS);
        self.w.write_unsigned_byte(ARRAY_LIST);
        marshall_size(&mut self.w, list.len() as i32);
        for a in list {
            self.write_address(a);
        }
    }

    /// `writeObject(Immutables.immutableMapCopy(Map<Address, Float>))`, or `null`. `cf` must already
    /// be in the Java map's iteration order (as `ReplicatedConsistentHash` stores it).
    pub fn write_immutable_capacity_map(&mut self, cf: Option<&CapacityFactors>) {
        match cf {
            None => self.write_null(),
            Some(m) if m.is_empty() => {
                self.write_internal_header(ID_MAPS);
                self.w.write_unsigned_byte(EMPTYMAP);
            }
            Some(m) if m.len() == 1 => {
                self.write_internal_header(ID_MAPS);
                self.w.write_unsigned_byte(SINGLETONMAP);
                self.write_address(&m.0[0].0);
                self.write_float_object(m.0[0].1);
            }
            Some(m) => {
                self.write_internal_header(ID_IMMUTABLE_MAP);
                marshall_size(&mut self.w, m.len() as i32);
                for (a, f) in &m.0 {
                    self.write_address(a);
                    self.write_float_object(*f);
                }
            }
        }
    }

    /// `writeObject(Float)`.
    pub fn write_float_object(&mut self, f: f32) {
        global::write_nullable(&mut self.w, &Value::Primitive(PrimitiveValue::FloatObj(f)));
    }

    /// `writeObject(int[])`.
    pub fn write_int_array(&mut self, a: &[i32]) {
        global::write_nullable(&mut self.w, &Value::Primitive(PrimitiveValue::IntArray(a.to_vec())));
    }

    /// `writeObject(ReplicatedConsistentHash)` (`ReplicatedConsistentHash.Externalizer`, `:476-504`).
    pub fn write_replicated_consistent_hash(&mut self, ch: &ReplicatedConsistentHash) {
        self.write_internal_header(ID_REPLICATED_CONSISTENT_HASH);
        let ch2 = ch.clone();
        self.write_reusable(Reusable::Rch(ch.clone()), true, move |o| {
            o.write_immutable_address_list(ch2.members());
            o.write_immutable_capacity_map(ch2.capacity_factors());
            o.write_immutable_address_list(ch2.members_without_state());
            o.write_int_array(ch2.primary_owners());
        });
    }
}

/// A `BytesObjectInput` plus the thread-local read-side `ReusableData`.
pub struct ObjectInput<'a> {
    /// The bytes read from.
    pub r: ByteReader<'a>,
    reuse: Option<Vec<Reusable>>,
}

impl<'a> ObjectInput<'a> {
    /// An input at the start of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        Self { r: ByteReader::new(buf), reuse: None }
    }

    pub(crate) fn expect_internal(&mut self, expected: u8, what: &'static str) -> Result<()> {
        let t = self.r.read_unsigned_byte()?;
        if t != ID_INTERNAL {
            return Err(Error::UnknownTag { what, value: t as i64 });
        }
        let id = self.r.read_unsigned_byte()?;
        if id != expected {
            return Err(Error::UnknownTag { what, value: id as i64 });
        }
        Ok(())
    }

    /// `InstanceReusingAdvancedExternalizer.readObject`.
    pub(crate) fn read_reusable(
        &mut self,
        has_children: bool,
        body: impl FnOnce(&mut Self) -> Result<Reusable>,
    ) -> Result<Reusable> {
        let outermost = has_children && self.reuse.is_none();
        if outermost {
            self.reuse = Some(Vec::new());
        }
        let result = (|| {
            let ty = self.r.read_unsigned_byte()?;
            match ty {
                ID_NO_REPEAT => {
                    let obj = body(self)?;
                    if let Some(d) = &mut self.reuse {
                        d.push(obj.clone());
                    }
                    Ok(obj)
                }
                ID_REPEAT_OBJECT_NEAR => {
                    let off = (self.r.read_unsigned_byte()? as i32) | -256;
                    self.cached(off)
                }
                ID_REPEAT_OBJECT_NEARISH => {
                    let off = (self.r.read_short()? as u16 as i32) | -65536;
                    self.cached(off)
                }
                ID_REPEAT_OBJECT_FAR => {
                    let idx = self.r.read_int()?;
                    let d = self.reuse.as_ref().ok_or_else(|| Error::Malformed("back-reference outside a context".into()))?;
                    d.get(idx as usize).cloned().ok_or_else(|| Error::Malformed("back-reference past end".into()))
                }
                other => Err(Error::UnknownTag { what: "InstanceReusing marker", value: other as i64 }),
            }
        })();
        if outermost {
            self.reuse = None;
        }
        result
    }

    fn cached(&self, offset: i32) -> Result<Reusable> {
        let d = self.reuse.as_ref().ok_or_else(|| Error::Malformed("back-reference outside a context".into()))?;
        let idx = offset + d.len() as i32;
        if idx < 0 {
            return Err(Error::Malformed("back-reference before start".into()));
        }
        d.get(idx as usize).cloned().ok_or_else(|| Error::Malformed("back-reference past end".into()))
    }

    /// `readObject()` for an `Address`.
    pub fn read_address(&mut self) -> Result<Address> {
        let t = self.r.read_unsigned_byte()?;
        if t != ID_INTERNAL {
            return Err(Error::UnknownTag { what: "address type byte", value: t as i64 });
        }
        match self.r.read_unsigned_byte()? {
            ID_JGROUPS_ADDRESS => {
                let r = self.read_reusable(false, |o| {
                    // ByteReader is Copy: peek the tail, let jgroups parse it, then consume what it used.
                    let mut peek = o.r;
                    let rest = peek.read_raw(peek.remaining())?;
                    let mut wr = WireReader::new(rest);
                    let a = read_address(&mut wr).ok_or_else(|| Error::Malformed("null jgroups address".into()))?;
                    let used = rest.len() - wr.remaining();
                    o.r.read_raw(used)?;
                    Ok(Reusable::Address(Address::JGroups(a)))
                })?;
                match r {
                    Reusable::Address(a) => Ok(a),
                    _ => Err(Error::Malformed("back-reference is not an address".into())),
                }
            }
            ID_PERSISTENT_UUID => Ok(Address::Persistent { msb: self.r.read_long()?, lsb: self.r.read_long()? }),
            other => Err(Error::UnknownTag { what: "address externalizer id", value: other as i64 }),
        }
    }

    /// `readObject()` for a `List<Address>`: `ImmutableListCopy`, `Collections` lists, or `ArrayList`.
    pub fn read_address_list(&mut self) -> Result<Vec<Address>> {
        let t = self.r.read_unsigned_byte()?;
        if t != ID_INTERNAL {
            return Err(Error::UnknownTag { what: "list type byte", value: t as i64 });
        }
        match self.r.read_unsigned_byte()? {
            ID_IMMUTABLE_LIST_COPY => self.read_sized_addresses(),
            ID_COLLECTIONS => match self.r.read_unsigned_byte()? {
                ARRAY_LIST => self.read_sized_addresses(),
                SINGLETON_LIST => Ok(vec![self.read_address()?]),
                EMPTY_LIST => Ok(Vec::new()),
                other => Err(Error::UnknownTag { what: "CollectionExternalizer kind", value: other as i64 }),
            },
            other => Err(Error::UnknownTag { what: "list externalizer id", value: other as i64 }),
        }
    }

    /// A nullable `List<Address>` (`writeObject(null)` is a single `ID_NULL`).
    pub fn read_nullable_address_list(&mut self) -> Result<Option<Vec<Address>>> {
        let mut peek = self.r;
        if peek.read_unsigned_byte()? == ID_NULL {
            self.r.read_unsigned_byte()?;
            return Ok(None);
        }
        Ok(Some(self.read_address_list()?))
    }

    fn read_sized_addresses(&mut self) -> Result<Vec<Address>> {
        let size = unmarshall_size(&mut self.r)?;
        if size < 0 {
            return Err(Error::Malformed("null address list".into()));
        }
        (0..size).map(|_| self.read_address()).collect()
    }

    fn read_float_object(&mut self) -> Result<f32> {
        match global::read_nullable(&mut self.r)? {
            Value::Primitive(PrimitiveValue::FloatObj(f)) => Ok(f),
            other => Err(Error::Malformed(format!("expected Float, got {other:?}"))),
        }
    }

    /// `readObject()` for a nullable `Map<Address, Float>`: built the way upstream builds it
    /// (`unmarshallMap(HashMap::new)`, i.e. a `HashMap(size)` filled in stream order).
    pub fn read_capacity_map(&mut self) -> Result<Option<CapacityFactors>> {
        let t = self.r.read_unsigned_byte()?;
        if t == ID_NULL {
            return Ok(None);
        }
        if t != ID_INTERNAL {
            return Err(Error::UnknownTag { what: "map type byte", value: t as i64 });
        }
        let entries = match self.r.read_unsigned_byte()? {
            ID_IMMUTABLE_MAP => self.read_sized_capacity_entries()?,
            ID_MAPS => match self.r.read_unsigned_byte()? {
                HASHMAP => self.read_sized_capacity_entries()?,
                SINGLETONMAP => vec![(self.read_address()?, self.read_float_object()?)],
                EMPTYMAP => Vec::new(),
                other => return Err(Error::UnknownTag { what: "MapExternalizer kind", value: other as i64 }),
            },
            other => return Err(Error::UnknownTag { what: "map externalizer id", value: other as i64 }),
        };
        let n = entries.len() as i32;
        Ok(Some(CapacityFactors::java_hash_map(entries, HashMapInit::Capacity(n))))
    }

    fn read_sized_capacity_entries(&mut self) -> Result<Vec<(Address, f32)>> {
        let size = unmarshall_size(&mut self.r)?;
        if size < 0 {
            return Err(Error::Malformed("null map".into()));
        }
        (0..size).map(|_| Ok((self.read_address()?, self.read_float_object()?))).collect()
    }

    /// `readObject()` for an `int[]`.
    pub fn read_int_array(&mut self) -> Result<Vec<i32>> {
        match global::read_nullable(&mut self.r)? {
            Value::Primitive(PrimitiveValue::IntArray(v)) => Ok(v),
            other => Err(Error::Malformed(format!("expected int[], got {other:?}"))),
        }
    }

    /// `readObject()` for a `ReplicatedConsistentHash`.
    pub fn read_replicated_consistent_hash(&mut self) -> Result<ReplicatedConsistentHash> {
        self.expect_internal(ID_REPLICATED_CONSISTENT_HASH, "RCH header")?;
        match self.read_reusable(true, |o| {
            let members = o.read_address_list()?;
            let cf = o.read_capacity_map()?;
            let without = o.read_address_list()?;
            let primary_owners = o.read_int_array()?;
            Ok(Reusable::Rch(ReplicatedConsistentHash::new(members, cf.as_ref(), without, primary_owners)))
        })? {
            Reusable::Rch(c) => Ok(c),
            _ => Err(Error::Malformed("back-reference is not an RCH".into())),
        }
    }
}
