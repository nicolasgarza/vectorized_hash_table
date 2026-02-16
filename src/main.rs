mod vector_hash;

use std::time::Instant;
use vector_hash::VectorHash;

fn main() {
    let mut map: VectorHash<u64, u64> = VectorHash::new();
    let mut checksum: u64 = 0;

    let n: u64 = 300_000;
    let start = Instant::now();

    for k in 0..n {
        map.put(k, k + 1);
    }

    for k in 0..n {
        if let Some(v) = map.get(&k) {
            checksum ^= *v;
        }
    }

    for k in 0..n {
        if let Some(old) = map.put(k, k * 2) {
            checksum ^= old;
        }
    }

    for k in (0..n).step_by(2) {
        if let Some(old) = map.delete(&k) {
            checksum ^= old;
        }
    }

    for k in (0..n).step_by(2) {
        map.put(k, k * 3);
    }

    for k in 0..n {
        if let Some(v) = map.get(&k) {
            checksum ^= *v;
        }
    }

    let duration = start.elapsed();
    println!("Test took {} ms", duration.as_millis());

    println!("checksum={checksum}");
}

#[cfg(test)]
mod tests {
    use super::VectorHash;

    #[derive(Hash, Eq, PartialEq, Debug)]
    struct UserId(u32);

    #[test]
    fn get_missing_returns_none() {
        let map: VectorHash<String, i32> = VectorHash::new();
        assert_eq!(map.get(&"missing".to_string()), None);
    }

    #[test]
    fn put_then_get_returns_value() {
        let mut map: VectorHash<String, i32> = VectorHash::new();
        assert_eq!(map.put("apple".to_string(), 50), None);
        assert_eq!(map.get(&"apple".to_string()).copied(), Some(50));
    }

    #[test]
    fn put_overwrite_returns_old_and_updates_value() {
        let mut map: VectorHash<&str, String> = VectorHash::new();

        assert_eq!(map.put("key", "first".to_string()), None);
        assert_eq!(map.put("key", "second".to_string()), Some("first".to_string()));
        assert_eq!(map.get(&"key").cloned(), Some("second".to_string()));
    }

    #[test]
    fn delete_existing_then_get_is_none_and_delete_returns_value() {
        let mut map: VectorHash<UserId, String> = VectorHash::new();

        map.put(UserId(9), "nine".into());

        assert_eq!(map.delete(&UserId(9)), Some("nine".into()));
        assert_eq!(map.get(&UserId(9)), None);
        assert_eq!(map.delete(&UserId(9)), None);
    }

    #[test]
    fn tombstone_does_not_break_probe_chain() {
        let mut map: VectorHash<String, String> = VectorHash::new();

        for i in 0..60 {
            map.put(format!("k{i}"), format!("v{i}"));
        }

        assert_eq!(map.delete(&"k20".to_string()), Some("v20".to_string()));

        for i in 0..60 {
            let key = format!("k{i}");
            if i == 20 {
                assert_eq!(map.get(&key), None);
            } else {
                assert_eq!(map.get(&key).cloned(), Some(format!("v{i}")));
            }
        }
    }

    #[test]
    fn reuse_deleted_slot_keeps_map_correct() {
        let mut map: VectorHash<&str, String> = VectorHash::new();

        map.put("a", "one".into());
        map.put("b", "two".into());
        map.put("c", "three".into());

        assert_eq!(map.delete(&"b"), Some("two".into()));
        assert_eq!(map.get(&"b"), None);

        assert_eq!(map.put("b", "new".into()), None);
        assert_eq!(map.get(&"b").cloned(), Some("new".into()));

        assert_eq!(map.get(&"a").cloned(), Some("one".into()));
        assert_eq!(map.get(&"c").cloned(), Some("three".into()));
    }

    #[test]
    fn resize_preserves_entries() {
        let mut map: VectorHash<UserId, String> = VectorHash::new();

        for i in 0..200 {
            map.put(UserId(i), format!("value{i}"));
        }

        for i in 0..200 {
            assert_eq!(
                map.get(&UserId(i)).cloned(),
                Some(format!("value{i}"))
            );
        }
    }
}

