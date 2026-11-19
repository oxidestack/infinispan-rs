//! Port of `org.infinispan.commons.hash.MurmurHash3` (commons 14.0.35.Final,
//! `commons/all/src/main/java/org/infinispan/commons/hash/MurmurHash3.java`), x64 variants only.
//!
//! Java's sign-extension quirks are kept on purpose: the `byte[]` tail does
//! `state.k2 ^= (long) key[tail + 14] << 48` and `state.k1 ^= key[tail + 0]` with signed bytes, so a
//! tail byte >= 0x80 smears ones into the upper lanes. Matching Java byte for byte is the point.

const C1: u64 = 0x87c3_7b91_1142_53d5;
const C2: u64 = 0x4cf5_ad43_2745_937f;
const H1_INIT: u64 = 0x9368_e53c_2f6a_f274;
const H2_INIT: u64 = 0x586d_cd20_8f7c_d3fd;
/// `MurmurHash3.INVALID_CHAR`
const INVALID_CHAR: i32 = b'?' as i32;
/// `hash(byte[])`, `hash(int)`, `hash(String)` all seed with 9001 (`:228-300`).
const SEED: i64 = 9001;

struct State {
    h1: u64,
    h2: u64,
    k1: u64,
    k2: u64,
    c1: u64,
    c2: u64,
}

impl State {
    fn new(seed: i64) -> Self {
        // `0x9368e53c2f6af274L ^ seed`: the int seed is sign-extended to long first.
        State {
            h1: H1_INIT ^ (seed as u64),
            h2: H2_INIT ^ (seed as u64),
            k1: 0,
            k2: 0,
            c1: C1,
            c2: C2,
        }
    }

    fn bmix(&mut self) {
        self.k1 = self.k1.wrapping_mul(self.c1);
        self.k1 = self.k1.rotate_left(23);
        self.k1 = self.k1.wrapping_mul(self.c2);
        self.h1 ^= self.k1;
        self.h1 = self.h1.wrapping_add(self.h2);

        self.h2 = self.h2.rotate_left(41);

        self.k2 = self.k2.wrapping_mul(self.c2);
        self.k2 = self.k2.rotate_left(23);
        self.k2 = self.k2.wrapping_mul(self.c1);
        self.h2 ^= self.k2;
        self.h2 = self.h2.wrapping_add(self.h1);

        self.h1 = self.h1.wrapping_mul(3).wrapping_add(0x52dc_e729);
        self.h2 = self.h2.wrapping_mul(3).wrapping_add(0x3849_5ab5);

        self.c1 = self.c1.wrapping_mul(5).wrapping_add(0x7b7d_159c);
        self.c2 = self.c2.wrapping_mul(5).wrapping_add(0x6bce_6396);
    }

    fn finish(&mut self, len_for_h2: u64) -> (u64, u64) {
        self.h2 ^= len_for_h2;
        self.h1 = self.h1.wrapping_add(self.h2);
        self.h2 = self.h2.wrapping_add(self.h1);
        self.h1 = fmix(self.h1);
        self.h2 = fmix(self.h2);
        self.h1 = self.h1.wrapping_add(self.h2);
        self.h2 = self.h2.wrapping_add(self.h1);
        (self.h1, self.h2)
    }
}

fn fmix(mut k: u64) -> u64 {
    k ^= k >> 33;
    k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
    k ^= k >> 33;
    k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    k ^= k >> 33;
    k
}

fn getblock(key: &[u8], i: usize) -> u64 {
    let mut v = 0u64;
    for j in 0..8 {
        v |= (key[i + j] as u64) << (8 * j);
    }
    v
}

/// Sign-extended `(long) key[i] << shift`.
fn sx(b: u8, shift: u32) -> u64 {
    ((b as i8 as i64) as u64) << shift
}

/// `MurmurHash3_x64_128(byte[], int)` returning `[h1, h2]`.
pub fn murmur_hash3_x64_128_bytes(key: &[u8], seed: i32) -> [i64; 2] {
    let mut s = State::new(seed as i64);
    for i in 0..key.len() / 16 {
        s.k1 = getblock(key, i * 2 * 8);
        s.k2 = getblock(key, (i * 2 + 1) * 8);
        s.bmix();
    }
    s.k1 = 0;
    s.k2 = 0;
    let tail = (key.len() >> 4) << 4;
    let rem = key.len() & 15;
    // Fall-through switch: every case <= rem applies, highest first (XOR commutes).
    if rem >= 9 {
        for n in 9..=rem {
            if n == 9 {
                s.k2 ^= (key[tail + 8] as i8 as i64) as u64;
            } else {
                s.k2 ^= sx(key[tail + n - 1], ((n - 9) * 8) as u32);
            }
        }
    }
    if rem >= 1 {
        for n in 1..=rem.min(8) {
            if n == 1 {
                s.k1 ^= (key[tail] as i8 as i64) as u64;
            } else {
                s.k1 ^= sx(key[tail + n - 1], ((n - 1) * 8) as u32);
            }
        }
        s.bmix();
    }
    let (h1, h2) = s.finish(key.len() as u64);
    [h1 as i64, h2 as i64]
}

/// `MurmurHash3_x64_64(byte[], int)`.
pub fn murmur_hash3_x64_64_bytes(key: &[u8], seed: i32) -> i64 {
    murmur_hash3_x64_128_bytes(key, seed)[0]
}

/// `MurmurHash3_x64_32(byte[], int)`.
pub fn murmur_hash3_x64_32_bytes(key: &[u8], seed: i32) -> i32 {
    (((murmur_hash3_x64_64_bytes(key, seed)) as u64) >> 32) as i32
}

/// `MurmurHash3_x64_128(long[], int)`.
pub fn murmur_hash3_x64_128_longs(key: &[i64], seed: i32) -> [i64; 2] {
    let mut s = State::new(seed as i64);
    for i in 0..key.len() / 2 {
        s.k1 = key[i * 2] as u64;
        s.k2 = key[i * 2 + 1] as u64;
        s.bmix();
    }
    if key.len() & 1 == 1 {
        let tail = key[key.len() - 1];
        s.k1 ^= tail as u64;
        s.bmix();
    }
    let (h1, h2) = s.finish((key.len() as u64).wrapping_mul(8));
    [h1 as i64, h2 as i64]
}

/// `MurmurHash3_x64_64(long[], int)`; this is what `SyncConsistentHashFactory.nodeHash` calls.
pub fn murmur_hash3_x64_64_longs(key: &[i64], seed: i32) -> i64 {
    murmur_hash3_x64_128_longs(key, seed)[0]
}

/// `MurmurHash3_x64_32(long[], int)`.
pub fn murmur_hash3_x64_32_longs(key: &[i64], seed: i32) -> i32 {
    ((murmur_hash3_x64_64_longs(key, seed) as u64) >> 32) as i32
}

/// `MurmurHash3.hash(byte[])` (`:228`).
pub fn hash_bytes(payload: &[u8]) -> i32 {
    murmur_hash3_x64_32_bytes(payload, SEED as i32)
}

/// `MurmurHash3.hash(long[])` (`:238`).
pub fn hash_longs(payload: &[i64]) -> i32 {
    murmur_hash3_x64_32_longs(payload, SEED as i32)
}

/// `MurmurHash3.hash(int)` (`:243-279`): the 4-byte input specialisation, little-endian bytes,
/// sign-extended in the tail exactly like the generic path.
pub fn hash_int(hashcode: i32) -> i32 {
    let b = hashcode.to_le_bytes();
    let mut s = State::new(SEED);
    s.k1 ^= sx(b[3], 24);
    s.k1 ^= sx(b[2], 16);
    s.k1 ^= sx(b[1], 8);
    s.k1 ^= (b[0] as i8 as i64) as u64;
    s.bmix();
    let (h1, _) = s.finish(4);
    (h1 >> 32) as i32
}

fn add_byte(s: &mut State, b: u8, len: usize) {
    let shift = (len & 0x7) * 8;
    let bb = (b as u64) << shift;
    if len & 0x8 == 0 {
        s.k1 |= bb;
    } else {
        s.k2 |= bb;
        if len & 0xf == 0xf {
            s.bmix();
            s.k1 = 0;
            s.k2 = 0;
        }
    }
}

/// Decode `s` the way `hashString` walks it: UTF-16 code units, a lone surrogate becomes `?`.
fn code_points_java(units: &[u16]) -> Vec<i32> {
    let mut out = Vec::with_capacity(units.len());
    let mut i = 0;
    while i < units.len() {
        let c1 = units[i];
        let is_high = (0xD800..0xDC00).contains(&c1);
        let is_low = (0xDC00..0xE000).contains(&c1);
        let cp = if !is_high && !is_low {
            c1 as i32
        } else if is_high {
            if i + 1 < units.len() && (0xDC00..0xE000).contains(&units[i + 1]) {
                let c2 = units[i + 1];
                i += 1;
                0x10000 + (((c1 as i32) - 0xD800) << 10) + ((c2 as i32) - 0xDC00)
            } else {
                INVALID_CHAR
            }
        } else {
            INVALID_CHAR
        };
        out.push(cp);
        i += 1;
    }
    out
}

/// `MurmurHash3_x64_64_String(String, long)` (`:316-`), which hashes the UTF-8 form of the string
/// without allocating it.
pub fn murmur_hash3_x64_64_string(units: &[u16], seed: i64) -> i64 {
    let mut s = State::new(seed);
    let mut byte_len = 0usize;
    for cp in code_points_java(units) {
        let mut emit = |b: u8| {
            add_byte(&mut s, b, byte_len);
            byte_len += 1;
        };
        if cp <= 0x7f {
            emit(cp as u8);
        } else if cp <= 0x07ff {
            emit((0xc0 | (0x1f & (cp >> 6))) as u8);
            emit((0x80 | (0x3f & cp)) as u8);
        } else if cp <= 0xffff {
            emit((0xe0 | (0x0f & (cp >> 12))) as u8);
            emit((0x80 | (0x3f & (cp >> 6))) as u8);
            emit((0x80 | (0x3f & cp)) as u8);
        } else {
            emit((0xf0 | (0x07 & (cp >> 18))) as u8);
            emit((0x80 | (0x3f & (cp >> 12))) as u8);
            emit((0x80 | (0x3f & (cp >> 6))) as u8);
            emit((0x80 | (0x3f & cp)) as u8);
        }
    }
    let saved_k1 = s.k1;
    let saved_k2 = s.k2;
    s.k1 = 0;
    s.k2 = 0;
    let rem = byte_len & 15;
    // `(long) ((byte) (saved >> (8*i))) << (8*i)`: signed byte, sign-extended.
    let lane = |saved: u64, i: u32| -> u64 { (((saved >> (8 * i)) as u8 as i8 as i64) as u64) << (8 * i) };
    if rem >= 9 {
        for n in 9..=rem {
            s.k2 ^= lane(saved_k2, (n - 9) as u32);
        }
    }
    if rem >= 1 {
        for n in 1..=rem.min(8) {
            s.k1 ^= lane(saved_k1, (n - 1) as u32);
        }
        s.bmix();
    }
    let (h1, _) = s.finish(byte_len as u64);
    h1 as i64
}

/// `MurmurHash3.hashString` (`:304`): `(int) (MurmurHash3_x64_64_String(s, 9001) >> 32)` (arithmetic shift).
pub fn hash_string(units: &[u16]) -> i32 {
    (murmur_hash3_x64_64_string(units, SEED) >> 32) as i32
}

/// Convenience over a Rust `&str` (UTF-16 view, so lone-surrogate behaviour cannot occur).
pub fn hash_str(s: &str) -> i32 {
    let units: Vec<u16> = s.encode_utf16().collect();
    hash_string(&units)
}

/// The shapes `MurmurHash3.hash(Object)` (`:285-301`) distinguishes.
#[derive(Debug, Clone, PartialEq)]
pub enum HashKey {
    /// `byte[]` or `WrappedBytes`.
    Bytes(Vec<u8>),
    /// `long[]`.
    Longs(Vec<i64>),
    /// UTF-16 code units of a `String`.
    Str(Vec<u16>),
    /// Anything else: `hash(o.hashCode())`.
    JavaHashCode(i32),
}

impl HashKey {
    /// The key of a `String`, as its UTF-16 code units.
    pub fn string(s: &str) -> Self {
        HashKey::Str(s.encode_utf16().collect())
    }
}

/// `MurmurHash3.hash(Object)`.
pub fn hash_object(key: &HashKey) -> i32 {
    match key {
        HashKey::Bytes(b) => hash_bytes(b),
        HashKey::Longs(l) => hash_longs(l),
        HashKey::Str(s) => hash_string(s),
        HashKey::JavaHashCode(h) => hash_int(*h),
    }
}
