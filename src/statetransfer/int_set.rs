//! `IntSet` as `IntSetsExternalization` writes it (`commons/.../util/IntSetsExternalization.java:34-74`),
//! for the segment sets of the state transfer commands. The Java class decides the wire form, so the
//! class is kept with the set: a `SmallIntSet` is a `BitSet.toByteArray()` behind a `marshallSize`
//! length (`SmallIntSet.java:486-493`), the one every state transfer command of upstream carries
//! (`InboundTransferTask.getSegments`, `IntSets.mutableCopyFrom`).

use std::collections::BTreeSet;

use crate::marshall::error::{Error, Result};
use crate::marshall::marshall_util::{marshall_size, unmarshall_size};
use crate::topology::wire::{ObjectInput, ObjectOutput};

/// The Java class an [`IntSet`] was, or is to be, written as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IntSetClass {
    /// `RangeSet` (0): `0..size`
    Range,
    /// `SmallIntSet` (1)
    #[default]
    Small,
    /// `EmptyIntSet` (2)
    Empty,
    /// `SingletonIntSet` (3)
    Singleton,
    /// `ConcurrentSmallIntSet` (4)
    Concurrent,
}

/// A set of segment numbers (`IntSet`), remembering which `IntSet` class it is written as.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct IntSet {
    set: BTreeSet<i32>,
    class: IntSetClass,
}

impl IntSet {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// The set of `values`.
    pub fn of(values: impl IntoIterator<Item = i32>) -> Self {
        Self { set: values.into_iter().collect(), class: IntSetClass::Small }
    }

    /// `0..n` as a `SmallIntSet` (every segment of a cache).
    pub fn range(n: i32) -> Self {
        Self::of(0..n)
    }

    /// Sets the class the set is written as.
    pub fn with_class(mut self, class: IntSetClass) -> Self {
        self.class = class;
        self
    }

    /// The class the set is written as.
    pub fn class(&self) -> IntSetClass {
        self.class
    }

    /// Adds `v`; false if it was there.
    pub fn insert(&mut self, v: i32) -> bool {
        self.set.insert(v)
    }

    /// Removes `v`; false if it was not there.
    pub fn remove(&mut self, v: i32) -> bool {
        self.set.remove(&v)
    }

    /// Whether `v` is in the set.
    pub fn contains(&self, v: i32) -> bool {
        self.set.contains(&v)
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.set.len()
    }

    /// Whether the set is empty.
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }

    /// The elements, ascending.
    pub fn iter(&self) -> impl Iterator<Item = i32> + '_ {
        self.set.iter().copied()
    }

    /// Whether every element of `other` is in the set.
    pub fn contains_all(&self, other: &IntSet) -> bool {
        other.set.is_subset(&self.set)
    }

    /// Removes the elements of `other`.
    pub fn remove_all(&mut self, other: &IntSet) {
        for v in &other.set {
            self.set.remove(v);
        }
    }

    /// Keeps only the elements of `other`.
    pub fn retain_all(&mut self, other: &IntSet) {
        self.set.retain(|v| other.set.contains(v));
    }

    /// Adds the elements of `other`.
    pub fn add_all(&mut self, other: &IntSet) {
        self.set.extend(other.set.iter().copied());
    }

    /// Removes every element.
    pub fn clear(&mut self) {
        self.set.clear();
    }

    /// `BitSet.toByteArray()`: little-endian, without trailing zero bytes.
    fn bit_set_bytes(&self) -> Vec<u8> {
        let Some(max) = self.set.iter().next_back() else { return Vec::new() };
        let mut bytes = vec![0u8; (*max as usize) / 8 + 1];
        for v in &self.set {
            bytes[*v as usize / 8] |= 1 << (*v % 8);
        }
        bytes
    }
}

impl ObjectOutput {
    /// `IntSetsExternalization.writeTo`.
    pub fn write_int_set(&mut self, s: &IntSet) {
        match s.class {
            IntSetClass::Range => {
                self.w.write_unsigned_byte(0);
                self.write_unsigned_int(s.len() as u32);
            }
            IntSetClass::Small => {
                self.w.write_unsigned_byte(1);
                let b = s.bit_set_bytes();
                marshall_size(&mut self.w, b.len() as i32);
                self.w.write_raw(&b);
            }
            IntSetClass::Empty => self.w.write_unsigned_byte(2),
            IntSetClass::Singleton => {
                self.w.write_unsigned_byte(3);
                self.write_unsigned_int(s.set.iter().next().copied().unwrap_or(0) as u32);
            }
            IntSetClass::Concurrent => {
                self.w.write_unsigned_byte(4);
                let b = s.bit_set_bytes();
                marshall_size(&mut self.w, b.len() as i32);
                self.w.write_raw(&b);
            }
        }
    }
}

impl ObjectInput<'_> {
    /// `IntSetsExternalization.readFrom`.
    pub fn read_int_set(&mut self) -> Result<IntSet> {
        let kind = self.r.read_unsigned_byte()?;
        let bits = |this: &mut Self, class| -> Result<IntSet> {
            let len = unmarshall_size(&mut this.r)?;
            let bytes = if len > 0 { this.r.read_raw(len as usize)?.to_vec() } else { Vec::new() };
            let mut s = IntSet::new().with_class(class);
            for (i, b) in bytes.iter().enumerate() {
                for bit in 0..8 {
                    if b & (1 << bit) != 0 {
                        s.insert((i * 8 + bit) as i32);
                    }
                }
            }
            Ok(s)
        };
        match kind {
            0 => Ok(IntSet::range(self.read_unsigned_int()? as i32).with_class(IntSetClass::Range)),
            1 => bits(self, IntSetClass::Small),
            2 => Ok(IntSet::new().with_class(IntSetClass::Empty)),
            3 => Ok(IntSet::of([self.read_unsigned_int()? as i32]).with_class(IntSetClass::Singleton)),
            4 => bits(self, IntSetClass::Concurrent),
            other => Err(Error::UnknownTag { what: "IntSet class", value: other as i64 }),
        }
    }
}
