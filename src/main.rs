mod vector_hash;

use vector_hash::VectorHash;

fn main() {
    let mut map = VectorHash::new();
    map.put(5, 5);
    println!("{:?}", map.get(5));

    map.delete(5);
    println!("{:?}", map.get(5));
}

#[cfg(test)]
mod tests {
    use super::VectorHash;

    #[test]
    fn get_missing_returns_none() {
        let map = VectorHash::new();
        assert_eq!(map.get(123), None);
    }

    #[test]
    fn put_then_get_returns_value() {
        let mut map = VectorHash::new();
        assert_eq!(map.put(5, 50), None);
        assert_eq!(map.get(5), Some(50));
    }

    #[test]
    fn put_overwrite_returns_old_and_updates_value() {
        let mut map = VectorHash::new();
        assert_eq!(map.put(7, 70), None);
        assert_eq!(map.put(7, 71), Some(70));
        assert_eq!(map.get(7), Some(71));
    }

    #[test]
    fn delete_existing_then_get_is_none_and_delete_returns_value() {
        let mut map = VectorHash::new();
        map.put(9, 90);

        assert_eq!(map.delete(9), Some(90));
        assert_eq!(map.get(9), None);
        assert_eq!(map.delete(9), None);
    }

    #[test]
    fn collision_linear_probing_basic() {
        let mut map = VectorHash::with_capacity(10);

        map.put(1, 100);
        map.put(11, 1100);
        map.put(21, 2100);

        assert_eq!(map.get(1), Some(100));
        assert_eq!(map.get(11), Some(1100));
        assert_eq!(map.get(21), Some(2100));
    }

    #[test]
    fn delete_in_collision_chain_does_not_break_other_keys() {
        let mut map = VectorHash::with_capacity(10);

        map.put(1, 100);
        map.put(11, 1100);
        map.put(21, 2100);

        assert_eq!(map.delete(11), Some(1100));
        assert_eq!(map.get(1), Some(100));
        assert_eq!(map.get(21), Some(2100));
        assert_eq!(map.get(11), None);
    }

    #[test]
    fn reuse_deleted_slot_keeps_map_correct() {
        let mut map = VectorHash::with_capacity(10);

        map.put(1, 10);
        map.put(11, 110);
        map.put(21, 210);

        assert_eq!(map.delete(11), Some(110));

        map.put(31, 310);

        assert_eq!(map.get(1), Some(10));
        assert_eq!(map.get(21), Some(210));
        assert_eq!(map.get(31), Some(310));
        assert_eq!(map.get(11), None);
    }

    #[test]
    fn resize_preserves_entries() {
        let mut map = VectorHash::with_capacity(10);

        for k in 0u64..9 {
            map.put(k, k + 1000);
        }

        for k in 0u64..9 {
            assert_eq!(map.get(k), Some(k + 1000));
        }
    }

    #[test]
    fn resize_with_collisions_preserves_entries() {
        let mut map = VectorHash::with_capacity(10);

        let keys = [1u64, 11, 21, 31, 41, 51, 61, 71, 81];
        for &k in &keys {
            map.put(k, k + 5000);
        }

        for &k in &keys {
            assert_eq!(map.get(k), Some(k + 5000));
        }
    }

    #[test]
    fn mixed_operations_smoke_test() {
        let mut map = VectorHash::with_capacity(10);

        map.put(1, 10);
        map.put(2, 20);
        map.put(12, 120);

        assert_eq!(map.get(12), Some(120));
        assert_eq!(map.delete(2), Some(20));
        assert_eq!(map.get(2), None);

        assert_eq!(map.put(2, 200), None);
        println!("reached");
        assert_eq!(map.get(2), Some(200));

        assert_eq!(map.put(1, 11), Some(10));
        assert_eq!(map.get(1), Some(11));
    }
}
