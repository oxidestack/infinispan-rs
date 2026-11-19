//! MurmurHash3 + HashFunctionPartitioner against tests/vectors/murmur_hash3.hex, produced by the
//! unmodified Infinispan 14.0.35.Final classes (upstream-tools/generate-topology-vectors.sh).

use infinispan::ch::hash_function_partitioner::HashFunctionPartitioner;
use infinispan::ch::murmur_hash3::*;

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect()
}

fn units(s: &str) -> Vec<u16> {
    (0..s.len() / 4).map(|i| u16::from_str_radix(&s[4 * i..4 * i + 4], 16).unwrap()).collect()
}

fn nums(s: &str) -> Vec<i64> {
    s.split(' ').map(|x| x.parse().unwrap()).collect()
}

#[test]
fn murmur_hash3_and_partitioner_match_java() {
    let text = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/vectors/murmur_hash3.hex")).unwrap();
    let p256 = HashFunctionPartitioner::new(256).unwrap();
    let p7 = HashFunctionPartitioner::new(7).unwrap();
    let mut count = 0;
    for line in text.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        let out = nums(f[2]);
        match f[0] {
            "bytes128" => {
                let (seed, hexs) = f[1].split_once(':').unwrap();
                let b = unhex(hexs);
                let seed: i32 = seed.parse().unwrap();
                let r = murmur_hash3_x64_128_bytes(&b, seed);
                assert_eq!([r[0], r[1], murmur_hash3_x64_32_bytes(&b, seed) as i64], [out[0], out[1], out[2]], "{line}");
            }
            "bytesHash" => {
                let k = HashKey::Bytes(unhex(f[1]));
                assert_eq!(hash_object(&k) as i64, out[0], "{line}");
                assert_eq!(p256.get_segment(&k) as i64, out[1], "{line}");
                assert_eq!(p7.get_segment(&k) as i64, out[2], "{line}");
            }
            "longs128" => {
                let (seed, ls) = f[1].split_once(':').unwrap();
                let k: Vec<i64> = ls.split(',').filter(|x| !x.is_empty()).map(|x| x.parse().unwrap()).collect();
                let seed: i32 = seed.parse().unwrap();
                let r = murmur_hash3_x64_128_longs(&k, seed);
                assert_eq!([r[0], r[1], murmur_hash3_x64_32_longs(&k, seed) as i64], [out[0], out[1], out[2]], "{line}");
            }
            "longsHash" => {
                let k: Vec<i64> = f[1].split(',').filter(|x| !x.is_empty()).map(|x| x.parse().unwrap()).collect();
                assert_eq!(hash_object(&HashKey::Longs(k)) as i64, out[0], "{line}");
            }
            "string" => {
                let k = HashKey::Str(units(f[1]));
                assert_eq!(hash_object(&k) as i64, out[0], "{line}");
                assert_eq!(p256.get_segment(&k) as i64, out[1], "{line}");
                assert_eq!(p7.get_segment(&k) as i64, out[2], "{line}");
            }
            "int" => {
                let i: i32 = f[1].parse().unwrap();
                let k = HashKey::JavaHashCode(i);
                assert_eq!(hash_int(i) as i64, out[0], "{line}");
                assert_eq!(p256.get_segment(&k) as i64, out[1], "{line}");
                assert_eq!(p7.get_segment(&k) as i64, out[2], "{line}");
            }
            "javaLong" => {
                let l: i64 = f[1].parse().unwrap();
                let hc = (l ^ ((l as u64) >> 32) as i64) as i32; // Long.hashCode
                let k = HashKey::JavaHashCode(hc);
                assert_eq!(hash_object(&k) as i64, out[0], "{line}");
                assert_eq!(p256.get_segment(&k) as i64, out[1], "{line}");
            }
            other => panic!("unknown vector kind {other}"),
        }
        count += 1;
    }
    assert!(count > 700, "vector file shrank: {count}");
}

#[test]
fn segment_size_and_end_hashes() {
    // Util.getSegmentSize(256) = 2^31 / 256
    let p = HashFunctionPartitioner::new(256).unwrap();
    let ends = p.segment_end_hashes();
    assert_eq!(ends[0], 1 << 23);
    assert_eq!(ends[255], 0);
    assert!(HashFunctionPartitioner::new(0).is_err());
}
